import threading
import time
from typing import Callable, Optional

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
from datafusion import SessionContext


EmitFn = Callable[[pa.RecordBatch, dict], None]


class WindowedOperator:
    def __init__(
        self,
        window_size_s: int,
        window_slide_s: int,
        query: str,
        output_schema: pa.Schema,
        emit_fn: EmitFn,
        table_name: str = "nexmark_data",
        timestamp_column: str = "date_time",
    ):
        if window_size_s <= 0:
            raise ValueError("window_size_s must be > 0")
        if window_slide_s <= 0:
            raise ValueError("window_slide_s must be > 0")
        self.window_size_ms = int(window_size_s * 1e3)
        self.window_slide_ms = int(window_slide_s * 1e3)
        self.query = query
        self.table_name = table_name
        self.ts_col = timestamp_column
        self.out_schema = output_schema
        self.emit = emit_fn

        self.active_sources: dict = {}
        self.state: dict[int, list[pa.RecordBatch]] = {}
        self.pending_windows: set[int] = set()
        self.lock = threading.Lock()

        self.windows_emitted = 0
        self.output_rows = 0
        self.output_bytes = 0
        self.input_rows = 0
        self.input_bytes = 0
        self.querying_time = 0.0
        self.sending_time = 0.0

    def register_source(self, source_key) -> None:
        with self.lock:
            self.active_sources[source_key] = {"watermark": 0, "is_finished": False}

    def mark_source_finished(self, source_key) -> None:
        with self.lock:
            if source_key in self.active_sources:
                self.active_sources[source_key]["is_finished"] = True

    def consume(self, source_key, batch: pa.RecordBatch, watermark: Optional[int]) -> None:
        self.input_rows += batch.num_rows
        self.input_bytes += batch.nbytes

        slide_batches = self.process_slides(batch)
        with self.lock:
            if source_key in self.active_sources:
                prev = self.active_sources[source_key]["watermark"]
                self.active_sources[source_key]["watermark"] = max(prev, watermark or 0)
            for slide_start, sub_batches in slide_batches.items():
                self.state.setdefault(slide_start, []).extend(sub_batches)
            self.register_pending_windows(slide_batches.keys())

        self.flush_ready()

    def flush_ready(self) -> int:
        return self.collect_windows_to_flush(force_all=False)

    def flush_all(self) -> int:
        return self.collect_windows_to_flush(force_all=True)

    def stats(self) -> dict:
        with self.lock:
            last_wm = max(
                (src["watermark"] for src in self.active_sources.values()),
                default=0,
            )
        return {
            "windows_emitted": self.windows_emitted,
            "input_rows": self.input_rows,
            "input_bytes": self.input_bytes,
            "output_rows": self.output_rows,
            "output_bytes": self.output_bytes,
            "querying": self.querying_time,
            "sending": self.sending_time,
            "last_watermark": last_wm,
        }

    def assign_windows_to_timestamp(self, timestamp: int) -> list[int]:
        windows = []
        latest_start = (timestamp // self.window_slide_ms) * self.window_slide_ms
        num_windows = (self.window_size_ms + self.window_slide_ms - 1) // self.window_slide_ms
        for i in range(num_windows):
            window_start = latest_start - (i * self.window_slide_ms)
            if window_start <= timestamp < window_start + self.window_size_ms:
                windows.append(window_start)
            else:
                break
        return windows

    def process_slides(self, batch: pa.RecordBatch) -> dict[int, list[pa.RecordBatch]]:
        slides: dict[int, list[pa.RecordBatch]] = {}
        if batch.num_rows == 0:
            return slides

        timestamps_np = batch.column(self.ts_col).to_numpy(zero_copy_only=True)
        slide_starts_np = (timestamps_np // self.window_slide_ms) * self.window_slide_ms

        change_idx = np.flatnonzero(slide_starts_np[1:] != slide_starts_np[:-1]) + 1
        start_idx = 0
        for end_idx in change_idx:
            sub_batch = batch.slice(start_idx, end_idx - start_idx)
            slides.setdefault(int(slide_starts_np[start_idx]), []).append(sub_batch)
            start_idx = end_idx

        sub_batch = batch.slice(start_idx, len(slide_starts_np) - start_idx)
        slides.setdefault(int(slide_starts_np[start_idx]), []).append(sub_batch)

        return slides

    def register_pending_windows(self, slide_starts) -> None:
        for slide_start in slide_starts:
            for window_start in self.assign_windows_to_timestamp(slide_start):
                self.pending_windows.add(window_start)

    def filter_batches_before(self, batches, window_end):
        filtered = []
        for batch in batches:
            mask = pc.less(batch.column(self.ts_col), window_end)
            filtered_batch = batch.filter(mask)
            if filtered_batch.num_rows:
                filtered.append(filtered_batch)
        return filtered

    def materialize_window_batches(self, window_end, window_slides):
        batches = []
        for slide_start, slide_batches in window_slides:
            if slide_start + self.window_slide_ms <= window_end:
                batches.extend(slide_batches)
            else:
                batches.extend(self.filter_batches_before(slide_batches, window_end))
        return batches

    def flush_window(self, batches: list[pa.RecordBatch], window_start: int, window_end: int) -> None:
        if not batches:
            return

        q_start = time.time()
        ctx = SessionContext()
        ctx.register_record_batches(self.table_name, partitions=[batches])
        df = ctx.sql(self.query)
        result_batches = df.collect()
        ctx.deregister_table(self.table_name)
        q_end = time.time()
        self.querying_time += q_end - q_start

        s_start = time.time()
        meta = {"window_start": window_start, "window_end": window_end}
        for result_batch in result_batches:
            casted = result_batch.cast(self.out_schema)
            self.emit(casted, meta)
            self.output_rows += casted.num_rows
            self.output_bytes += casted.nbytes
        self.sending_time += time.time() - s_start

        self.windows_emitted += 1

    def collect_windows_to_flush(self, force_all: bool) -> int:
        to_flush = []

        with self.lock:
            active_watermarks = [
                src["watermark"]
                for src in self.active_sources.values()
                if not src["is_finished"]
            ]
            if force_all or not active_watermarks:
                ready_windows = sorted(self.pending_windows)
                global_watermark = None
            else:
                global_watermark = min(active_watermarks)
                ready_windows = sorted(
                    window_start
                    for window_start in self.pending_windows
                    if window_start + self.window_size_ms <= global_watermark
                )

            for window_start in ready_windows:
                window_end = window_start + self.window_size_ms
                window_slides = []
                slide_start = window_start
                while slide_start < window_end:
                    slide_batches = self.state.get(slide_start)
                    if slide_batches:
                        window_slides.append((slide_start, slide_batches))
                    slide_start += self.window_slide_ms

                if window_slides:
                    to_flush.append((window_start, window_end, window_slides))
                self.pending_windows.discard(window_start)

            if global_watermark is None:
                self.state.clear()
            else:
                expired_slides = [
                    slide_start
                    for slide_start in self.state.keys()
                    if slide_start + self.window_size_ms <= global_watermark
                ]
                for slide_start in expired_slides:
                    self.state.pop(slide_start, None)

        emitted = 0
        for window_start, window_end, window_slides in to_flush:
            batches = self.materialize_window_batches(window_end, window_slides)
            if batches:
                self.flush_window(batches, window_start, window_end)
                emitted += 1
        return emitted
