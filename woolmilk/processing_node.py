import argparse
import json
import threading
import time

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.flight
from datafusion import SessionContext


class ProcessingNode(pa.flight.FlightServerBase):
    def __init__(self, location, forward_node, sql_query, schema_json, window_size, window_slide):
        super().__init__(location)
        self.forwarding_client = pa.flight.FlightClient(f"grpc://{forward_node}")
        self.query = sql_query
        self.default_table_name = "nexmark_data"
        self.logs = []
        self.logs_lock = threading.Lock()

        if not schema_json:
            raise ValueError("Schema is mandatory. Please provide a valid schema.")

        self.predefined_schema = self._parse_schema(schema_json)
        print(self.predefined_schema)

        self.window_size = int(window_size * 1e3) # convert to ms
        self.window_slide = int(window_slide * 1e3) # convert to ms

        # Track sources with their status and watermarks
        self.active_sources = {}
        # Slide state keyed by slide start, plus pending window starts to flush.
        self.state = {}
        self.pending_windows = set()

        self.lock = threading.Lock()


    def _parse_schema(self, schema_json: str) -> pa.Schema:
        schema_dict = json.loads(schema_json)
        fields = []
        for field in schema_dict.get("fields", []):
            field_name = field["name"]
            field_type = field["type"]

            if field_type == "int64":
                pa_type = pa.int64()
            elif field_type == "string":
                pa_type = pa.string()
            elif field_type == "float64":
                pa_type = pa.float64()
            else:
                pa_type = pa.string()

            fields.append(pa.field(field_name, pa_type))

        return pa.schema(fields)


    def do_action(self, context, action):
        if action.type == "get_logs":
            with self.logs_lock:
                logs = self.logs
            yield pyarrow.flight.Result(json.dumps(logs).encode("utf-8"))
        elif action.type == "delete_logs":
            with self.logs_lock:
                self.logs = []
        else:
            raise NotImplementedError(f"Unknown action: {action.type}")


    def _assign_windows_to_timestamp(self, timestamp:int) -> list[int]:
        # Compute all windows an event belongs to
        windows = []

        latest_start = (timestamp // self.window_slide) * self.window_slide

        num_windows = (self.window_size + self.window_slide - 1) // self.window_slide
        for i in range(num_windows):
            window_start = latest_start - (i * self.window_slide)
            if window_start <= timestamp < window_start + self.window_size:
                windows.append(window_start)
            else:
                break

        return windows


    def _process_slides(self, batch: pa.RecordBatch) -> dict[int, list[pa.RecordBatch]]:
        # Group tuples into state to avoid duplicating tuples into overlapping windows.
        slides = {}
        if batch.num_rows == 0:
            return slides

        timestamps = batch.column("date_time")
        timestamps_np = timestamps.to_numpy(zero_copy_only=True)
        slide_starts_np = (timestamps_np // self.window_slide) * self.window_slide

        change_idx = np.flatnonzero(slide_starts_np[1:] != slide_starts_np[:-1]) + 1
        start_idx = 0
        for end_idx in change_idx:
            sub_batch = batch.slice(start_idx, end_idx - start_idx)
            slides.setdefault(int(slide_starts_np[start_idx]), []).append(sub_batch)
            start_idx = end_idx

        sub_batch = batch.slice(start_idx, len(slide_starts_np) - start_idx)
        slides.setdefault(int(slide_starts_np[start_idx]), []).append(sub_batch)

        return slides


    def _register_pending_windows(self, slide_starts) -> None:
        for slide_start in slide_starts:
            for window_start in self._assign_windows_to_timestamp(slide_start):
                self.pending_windows.add(window_start)


    def _filter_batches_before(self, batches, window_end):
        filtered = []
        for batch in batches:
            timestamps = batch.column("date_time")
            mask = pc.less(timestamps, window_end)
            filtered_batch = batch.filter(mask)
            if filtered_batch.num_rows:
                filtered.append(filtered_batch)
        return filtered


    def _materialize_window_batches(self, window_end, window_slides):
        batches = []
        for slide_start, slide_batches in window_slides:
            if slide_start + self.window_slide <= window_end:
                batches.extend(slide_batches)
            else:
                batches.extend(self._filter_batches_before(slide_batches, window_end))
        return batches


    def _flush_window(self, batches: list[pa.RecordBatch], forward_writer):
        # Run query on materialized window once it's ready and forward the results
        if not batches:
            return

        ctx = SessionContext()
        ctx.register_record_batches("nexmark_data", partitions=[batches])

        df = ctx.sql(self.query)

        for batch in df.collect():
            forward_writer.write_batch(batch.cast(self.predefined_schema))

        ctx.deregister_table("nexmark_data")

    def _collect_windows_to_flush(self, forward_writer):
        # Get completed windows by comparing to the global watermark
        to_flush = []

        with self.lock:
            active_watermarks = [src["watermark"] for src in self.active_sources.values() if not src["is_finished"]]
            if not active_watermarks:
                # All sources are finished; flush all remaining windows
                ready_windows = sorted(self.pending_windows)
                global_watermark = None
            else:
                global_watermark = min(active_watermarks)
                ready_windows = sorted(
                    window_start
                    for window_start in self.pending_windows
                    if window_start + self.window_size <= global_watermark
                )

            for window_start in ready_windows:
                window_end = window_start + self.window_size
                window_slides = []
                slide_start = window_start
                while slide_start < window_end:
                    slide_batches = self.state.get(slide_start)
                    if slide_batches:
                        window_slides.append((slide_start, slide_batches))
                    slide_start += self.window_slide

                if window_slides:
                    to_flush.append((window_end, window_slides))
                self.pending_windows.discard(window_start)

            if global_watermark is None:
                self.state.clear()
            else:
                expired_slides = [
                    slide_start
                    for slide_start in self.state.keys()
                    if slide_start + self.window_size <= global_watermark
                ]
                for slide_start in expired_slides:
                    self.state.pop(slide_start, None)

        for window_end, window_slides in to_flush:
            batches = self._materialize_window_batches(window_end, window_slides)
            self._flush_window(batches, forward_writer)


    def process_windowed(self, source_key, reader, forward_writer):
        # Group tuples into state, materialize windows when ready.
        if source_key is None:
            raise ValueError(
                "Windowed processing requires source_node_id and thread_id in the descriptor."
            )
        with self.lock:
            self.active_sources[source_key] = {"watermark": 0, "is_finished": False}

        start = time.time()

        for chunk in reader:
            batch = chunk.data

            bytes_meta = chunk.app_metadata.to_pybytes()
            watermark = int(bytes_meta.decode("utf-8"))

            slide_batches = self._process_slides(batch)
            with self.lock:
                self.active_sources[source_key]["watermark"] = watermark
                for slide_start, sub_batches in slide_batches.items():
                    self.state.setdefault(slide_start, []).extend(sub_batches)
                self._register_pending_windows(slide_batches.keys())

            self._collect_windows_to_flush(forward_writer)

        with self.lock:
            self.active_sources[source_key]["is_finished"] = True

        self._collect_windows_to_flush(forward_writer)

        end = time.time()
        duration = end - start

        return {
            "received_bytes": None,
            "start_time": start,
            "duration": duration,
            "MBps": None,
            "Gbps": None,
            "receiving": None,
            "querying": None,
            "sending": None,
            "forward_times": None,
        }



    def process_default(self, reader, forward_writer):
        # Stateless mode
        ctx = SessionContext()
        total_bytes = 0
        forwarding_times = []
        cost_break_down = {"receiving": [], "querying": [], "sending": []}
        forward_start = start = time.time()

        for chunk in reader:
            batch = chunk.data

            processing_start = time.time()
            ctx.register_record_batches(self.default_table_name, [[batch]])

            result_df = ctx.sql(self.query)

            result = result_df.collect()
            processing_end = time.time()

            for result_batch in result:
                forward_writer.write_batch(result_batch)
                total_bytes += result_batch.nbytes

            ctx.deregister_table(self.default_table_name)
            forward_end = time.time()
            forwarding_times.append((forward_start, forward_end))

            cost_break_down["receiving"].append(processing_start - forward_start)
            cost_break_down["querying"].append(processing_end - processing_start)
            cost_break_down["sending"].append(forward_end - processing_end)

            forward_start = forward_end

        forward_writer.done_writing()
        end = time.time()

        duration = end - start
        gbps = (total_bytes * 8) / (duration * 1000**3)
        mbps = total_bytes / (duration * 1000**2)

        return {
            "received_bytes": total_bytes,
            "start_time": start,
            "duration": duration,
            "MBps": f"{mbps:.2f}",
            "Gbps": f"{gbps:.4f}",
            "receiving": sum(cost_break_down["receiving"]),
            "querying": sum(cost_break_down["querying"]),
            "sending": sum(cost_break_down["sending"]),
            "forward_times": forwarding_times,
        }


    def do_put(self, context, descriptor, reader, writer):
        # data for path info
        experiment_id = None
        iteration_id = None
        source_node_id = None
        thread_id = None

        incoming_path_info = {}

        try:
            incoming_path_info = json.loads(descriptor.path[0].decode("utf-8"))
            if isinstance(incoming_path_info, dict):
                experiment_id = incoming_path_info.get("experiment_id")
                iteration_id = incoming_path_info.get("iteration_id")
                source_node_id = incoming_path_info.get("source_node_id")
                thread_id = incoming_path_info.get("thread_id")

        except (json.JSONDecodeError, UnicodeDecodeError, AttributeError):
            pass

        # we forward same path information to the next node
        forwarded_path_info = json.dumps(incoming_path_info)

        forward_writer, _ = self.forwarding_client.do_put(
            pa.flight.FlightDescriptor.for_path(forwarded_path_info),
            self.predefined_schema,
        )

        if window_size != 0:
            if source_node_id is None or thread_id is None:
                source_key = None
            else:
                source_key = (source_node_id, thread_id)
            logs = self.process_windowed(source_key, reader, forward_writer)
        else:
            logs = self.process_default(reader, forward_writer)

        log = {
            "experiment_id": experiment_id,
            "iteration_id": iteration_id,
            "source_node_id": source_node_id,
            "thread_id": thread_id,
        }

        log.update(logs)
        with self.logs_lock:
            self.logs.append(log)
        log_str = json.dumps(log)
        print(log_str)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="WoolMilk Processing Node")
    parser.add_argument(
        "--port",
        type=int,
        default=8010,
        help="Port to run the WoolMilk processing node",
    )
    parser.add_argument(
        "--forward-node",
        type=str,
        default="localhost:8020",
        help="Address of the node to forward data to (host:port)",
    )
    parser.add_argument(
        "--query",
        type=str,
        default="SELECT * FROM nexmark_data",
        help="SQL query to run on incoming batches",
    )
    parser.add_argument(
        "--query-result-schema",
        type=str,
        required=True,
        help="JSON schema definition for the data (required)",
    )
    parser.add_argument(
        "--window-size",
        type=int,
        default=0
    )
    parser.add_argument(
        "--window-slide",
        type=int,
        default=0
    )

    args = parser.parse_args()

    print("\n" + "=" * 40)
    print(" WoolMilk Processing Node Parameters")
    print("=" * 40)
    print(f" Port           : {args.port}")
    print(f" Forward Node   : {args.forward_node}")
    print(f" SQL Query      : {args.query}")
    print(f" Schema         : {args.query_result_schema}")
    print(f" Window Size    : {args.window_size}")
    print(f" Window Slide   : {args.window_slide}")
    print("=" * 40 + "\n")

    port = args.port
    forward_node = args.forward_node
    sql_query = args.query
    schema_json = args.query_result_schema
    window_size = args.window_size
    window_slide = args.window_slide

    processing_node = ProcessingNode(
        f"grpc://0.0.0.0:{port}", forward_node, sql_query, schema_json, window_size, window_slide
    )
    print(f"WoolMilk processing node running on port {port}")
    processing_node.serve()
