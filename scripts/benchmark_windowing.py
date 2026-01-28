import argparse
import resource
import sys
import time
from multiprocessing import Pipe, Process
from typing import Optional

import os

import pyarrow as pa
from datafusion import SessionContext

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from woolmilk.data_generator import NexmarkDataGenerator


def format_bytes(value) -> str:
    units = ["B", "KB", "MB", "GB"]
    size = float(value)
    for unit in units:
        if size < 1024 or unit == units[-1]:
            return f"{size:.2f} {unit}"
        size /= 1024
    return "None"


def generate_table(chunk_size, no_records) -> pa.Table:
    generator = NexmarkDataGenerator(
        chunk_size=chunk_size,
        no_records=no_records,
        event_type="bid",
        executable="nexmark",
    )

    tables = []
    for _, _, table, _ in generator.generate():
            tables.append(table)

    return pa.concat_tables(tables)


def assign_windows_to_timestamp(timestamp, window_size, window_slide) -> list[int]:
    window_size = int(window_size)
    window_slide = int(window_slide)
    windows = []

    latest_start = (timestamp // window_slide) * window_slide

    num_windows = (window_size + window_slide - 1) // window_slide
    for i in range(num_windows):
        window_start = latest_start - (i * window_slide)
        if window_start <= timestamp < window_start + window_size:
            windows.append(window_start)
        else:
            break

    return windows


def process_windowed(table, window_size, window_slide):
    state = {}
    processing_time = 0.0
    batch_count = 0

    table = table.to_batches()
    for chunk in table:
        batch = chunk
        processing_start = time.perf_counter()

        timestamps = batch.column("date_time")
        timestamps_list = timestamps.to_pylist()

        to_append = {}
        current_windows = None
        start_idx = 0

        for idx, ts in enumerate(timestamps_list):
            event_windows = assign_windows_to_timestamp(ts, window_size, window_slide)

            if current_windows is not None and event_windows != current_windows:
                sub_batch = batch.slice(start_idx, idx - start_idx)
                for win_start in current_windows:
                    to_append.setdefault(win_start, []).append(sub_batch)
                start_idx = idx
            current_windows = event_windows

        if current_windows is not None and start_idx < len(timestamps_list):
            sub_batch = batch.slice(start_idx, len(timestamps_list) - start_idx)
            for win_start in current_windows:
                to_append.setdefault(win_start, []).append(sub_batch)

        for win_start, sub_batches in to_append.items():
            state.setdefault(win_start, []).extend(sub_batches)

        processing_time += time.perf_counter() - processing_start
        batch_count += 1

    return state, processing_time, batch_count


def run_query_over_windows(state, query):
    if not query:
        return 0.0

    query_time = 0.0
    for window_start in state.keys():
        batches = state[window_start]
        if not batches:
            continue

        query_start = time.perf_counter()
        ctx = SessionContext()
        ctx.register_record_batches("window_table", partitions=[batches])
        df = ctx.sql(query)

        #print(f"window_start={window_start}")
        #for batch in df.collect():
        #    print(batch)

        ctx.deregister_table("window_table")
        query_time += time.perf_counter() - query_start

    return query_time


def run_once(records, chunk_size, window_size_s, window_slide_s, query, conn=None):
    if window_size_s <= 0 or window_slide_s <= 0:
        raise ValueError("Window size and slide must be positive.")

    window_size_ms = window_size_s * 1e3
    window_slide_ms = window_slide_s * 1e3

    table = generate_table(chunk_size, records)

    if table.num_rows == 0:
        raise ValueError("Generated empty table; check nexmark generator settings.")

    input_bytes = table.nbytes
    state, processing_time, batch_count = process_windowed(
        table, window_size_ms, window_slide_ms
    )

    query_time = 0.0
    if query:
        query_time = run_query_over_windows(state, query)
        processing_time += query_time

    total_rows = table.num_rows
    windows = len(state)

    peak_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    peak_bytes = int(peak_kb) * 1024

    result = {
        "records": records,
        "chunk_size": chunk_size,
        "window_size_s": window_size_s,
        "window_slide_s": window_slide_s,
        "rows": total_rows,
        "batches": batch_count,
        "windows": windows,
        "input_bytes": input_bytes,
        "processing_seconds": processing_time,
        "peak_rss_kb": peak_kb,
        "peak_rss_bytes": peak_bytes,
        "query_seconds": query_time,
    }

    if conn is not None:
        conn.send(result)
        conn.close()
        return

    print_result_labels(result, include_query=bool(query))


def parse_int_list(value: str) -> list[int]:
    items = [v.strip() for v in value.split(",") if v.strip()]
    return [int(v) for v in items]


def print_result_labels(result, include_query, include_config = False) -> None:
    labels = []
    if include_config:
        labels.extend(
            [
                ("Records", f"{result['records']}"),
                ("Chunk size", f"{result['chunk_size']}"),
                ("Window size", f"{result['window_size_s']} s"),
                ("Window slide", f"{result['window_slide_s']} s"),
            ]
        )
    labels.extend(
        [
        ("Total number of tuples", f"{result['rows']}"),
        ("Number of batches", f"{result['batches']}"),
        ("Total size of input", f"{format_bytes(result['input_bytes'])}"),
        ("Number of windows", f"{result['windows']}"),
        ("Total processing time", f"{result['processing_seconds']:.4f} s"),
        ("Query processing time", f"{result['query_seconds']:.4f} s") if include_query else None,
        ("Peak memory usage", f"{format_bytes(result['peak_rss_bytes'])}"),
        ]
    )
    labels = [item for item in labels if item is not None]
    label_width = max(len(label) for label, _ in labels)
    for label, value in labels:
        print(f"{label:<{label_width}} = {value}")


def run_in_subprocess(records, chunk_size, window_size_s, window_slide_s, query: Optional[str] = None,) -> dict:
    parent_conn, child_conn = Pipe()
    proc = Process(
        target=run_once,
        args=(records, chunk_size, window_size_s, window_slide_s, query, child_conn),
    )
    proc.start()
    result = parent_conn.recv()
    proc.join()
    return result


def main():
    parser = argparse.ArgumentParser(description="Benchmark windowing logic")
    parser.add_argument("--records", type=int, default=1000000, help="Total records to generate")
    parser.add_argument("--chunk-size", type=int, default=10000, help="Generator chunk size")
    parser.add_argument("--window-size", type=int, default=10, help="Window size in seconds")
    parser.add_argument("--window-slide", type=int, default=2, help="Window slide in seconds")
    parser.add_argument("--query", type=str, help="Optional SQL query to run on each window")
    parser.add_argument("--sweep-records", type=str, help="Comma-separated list of record counts")
    parser.add_argument("--sweep-chunk-size", type=str, help="Comma-separated list of chunk sizes")
    parser.add_argument("--sweep-window-size", type=str, help="Comma-separated list of window sizes (seconds)")
    parser.add_argument("--sweep-window-slide", type=str, help="Comma-separated list of window slides (seconds)")
    args = parser.parse_args()

    sweep_records = parse_int_list(args.sweep_records) if args.sweep_records else [args.records]
    sweep_chunk = parse_int_list(args.sweep_chunk_size) if args.sweep_chunk_size else [args.chunk_size]
    sweep_window_size = parse_int_list(args.sweep_window_size) if args.sweep_window_size else [args.window_size]
    sweep_window_slide = parse_int_list(args.sweep_window_slide) if args.sweep_window_slide else [args.window_slide]

    is_sweep = any(
        [
            args.sweep_records,
            args.sweep_chunk_size,
            args.sweep_window_size,
            args.sweep_window_slide,
        ]
    )

    if not is_sweep:
        run_once(args.records, args.chunk_size, args.window_size, args.window_slide, args.query)
        return

    for records in sweep_records:
        for chunk_size in sweep_chunk:
            for window_size_s in sweep_window_size:
                for window_slide_s in sweep_window_slide:
                    result = run_in_subprocess(records, chunk_size, window_size_s, window_slide_s, None)
                    print_result_labels(result, include_query=False, include_config=True)
                    print()


if __name__ == "__main__":
    main()
