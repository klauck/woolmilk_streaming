import argparse
import json
import threading
import time

import pyarrow as pa
import pyarrow.flight
from datafusion import SessionContext


class ProcessingNode(pa.flight.FlightServerBase):
    def __init__(self, location, forward_node, sql_query, schema_json, enable_windowing, window_size, window_slide=None):
        super().__init__(location)
        self.forwarding_client = pa.flight.FlightClient(f"grpc://{forward_node}")
        self.query = sql_query
        self.default_table_name = "nexmark_data"

        if not schema_json:
            raise ValueError("Schema is mandatory. Please provide a valid schema.")

        self.predefined_schema = self._parse_schema(schema_json)
        print(self.predefined_schema)

        self.active_sources = {}

        self.state = {}
        self.enable_windowing = enable_windowing
        self.window_size = window_size * 1000
        self.window_slide = (window_slide * 1000) if window_slide else self.window_size

        self.lock = threading.Lock()
        
        if self.enable_windowing:
            self.flusher_thread = threading.Thread(target=self.flusher, daemon=True)
            self.flusher_thread.start()

    def _parse_schema(self, schema_json):
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
        source_id = action.body.to_pybytes().decode('utf-8')
        if action.type == "register":
            with self.lock:
                self.active_sources[source_id] = {"watermark": 0, "is_finished": False}
            yield pa.flight.Result(b"registered")

        elif action.type == "complete":
            with self.lock:
                self.active_sources[source_id]["is_finished"] = True
            yield pa.flight.Result(b"completed")

        else:
            raise NotImplementedError

    def assign_windows(self, timestamp):
        windows = []
        latest_start = (timestamp // self.window_slide) * self.window_slide

        current_start = latest_start
        while current_start >= 0:
            window_end = current_start + self.window_size
            if current_start <= timestamp < window_end:
                windows.append(current_start)
                current_start -= self.window_slide
            else:
                break

        return windows

    def flush_window(self, batches):
        if not batches:
            return

        ctx = SessionContext()
        ctx.register_record_batches("window_table", partitions=[batches])

        df = ctx.sql(self.query)

        forward_writer, _ = self.forwarding_client.do_put(
            pa.flight.FlightDescriptor.for_path(self.query or self.default_table_name),
            df.schema()
        )

        for batch in df.collect():
            forward_writer.write_batch(batch)

        forward_writer.done_writing()
        ctx.deregister_table("window_table")

    def flusher(self):
        while True:
            to_flush = []

            with self.lock:
                active_watermarks = [src["watermark"] for src in self.active_sources.values() if not src["is_finished"]]

                if active_watermarks:
                    global_watermark = min(active_watermarks)

                    for window_start in list(self.state.keys()):
                        window_end = window_start + self.window_size
                        if window_end <= global_watermark:
                            to_flush.append((self.state.pop(window_start)))

            for batches in to_flush:
                self.flush_window(batches)

            time.sleep(0.1)

    def do_put(self, context, descriptor, reader, writer):
        if self.enable_windowing:
            for chunk in reader:
                source_id = chunk.app_metadata.decode("utf-8")
                batch = chunk.data
                watermark = batch.column("date_time")[-1].as_py()
                with self.lock:
                    self.active_sources[source_id]["watermark"] = watermark

                # Get timestamps (Unix timestamps in ms)
                timestamps = batch.column("date_time").cast(pa.int64())
                timestamps_list = timestamps.to_pylist()

                # Group consecutive events with the same set of windows
                current_windows = None
                start_idx = 0
                
                for idx, ts in enumerate(timestamps_list):
                    event_windows = self.assign_windows(ts)
                    
                    # If windows changed, flush the previous group
                    if current_windows is not None and event_windows != current_windows:
                        sub_batch = batch.slice(start_idx, idx - start_idx)
                        # Add this sub_batch to all its windows
                        with self.lock:
                            for win_start in current_windows:
                                self.state.setdefault(win_start, []).append(sub_batch)
                        start_idx = idx
                    
                    current_windows = event_windows
                
                # Handle the last group
                if current_windows is not None and start_idx < len(timestamps_list):
                    sub_batch = batch.slice(start_idx, len(timestamps_list) - start_idx)
                    with self.lock:
                        for win_start in current_windows:
                            self.state.setdefault(win_start, []).append(sub_batch)

        else:
            ctx = SessionContext()

            forward_writer, _ = self.forwarding_client.do_put(
                pa.flight.FlightDescriptor.for_path(self.query or self.default_table_name),
                schema=self.predefined_schema,
            )

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
            gbps = (total_bytes * 8) / (duration * 1000 ** 3)
            mbps = total_bytes / (duration * 1000 ** 2)

            print(
                f'WM_LOG= {{"received_bytes": {total_bytes}, "start_time": {start},'
                f' "duration": {duration}, "MBps": {mbps:.2f}, "Gbps": {gbps:.4f}}}'
            )
            print("  receiving: ", sum(cost_break_down["receiving"]))
            print("  querying: ", sum(cost_break_down["querying"]))
            print("  sending: ", sum(cost_break_down["sending"]))
            print("forward_times = ", forwarding_times)


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
        "--enable-windowing",
        action="store_true",
    )
    parser.add_argument(
        "--window-size",
        type=int,
        default=3,
        help="Window size to use (in seconds)",
    )
    parser.add_argument(
        "--window-slide",
        type=int,
        default=None,
        help="Slide interval for windows (in seconds). If not specified, defaults to window-size (tumbling).",
    )
    args = parser.parse_args()

    print("\n" + "=" * 40)
    print(" WoolMilk Processing Node Parameters")
    print("=" * 40)
    print(f" Port           : {args.port}")
    print(f" Forward Node   : {args.forward_node}")
    print(f" SQL Query      : {args.query}")
    print(f" Schema         : {args.query_result_schema}")
    print(f" Windowing      : {args.enable_windowing}")
    if args.enable_windowing:
        print(f" Window Size    : {args.window_size}s")
        slide = args.window_slide if args.window_slide else args.window_size
        print(f" Window Slide   : {slide}s")
    print("=" * 40 + "\n")

    port = args.port
    forward_node = args.forward_node
    sql_query = args.query
    schema_json = args.query_result_schema
    enable_windowing = args.enable_windowing
    window_size = args.window_size
    window_slide = args.window_slide

    processing_node = ProcessingNode(
        f"grpc://0.0.0.0:{port}", 
        forward_node, 
        sql_query, 
        schema_json, 
        enable_windowing, 
        window_size,
        window_slide=window_slide
    )
    print(f"WoolMilk processing node running on port {port}")
    processing_node.serve()
