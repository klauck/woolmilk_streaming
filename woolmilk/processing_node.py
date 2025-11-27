import argparse
import json
import threading
import time

import pyarrow as pa
import pyarrow.flight
from datafusion import SessionContext


class ProcessingNode(pa.flight.FlightServerBase):
    def __init__(self, location, forward_node, sql_query, schema_json, stateful, column_filter, window_size, window_slide):
        super().__init__(location)
        self.forwarding_client = pa.flight.FlightClient(f"grpc://{forward_node}")
        self.query = sql_query
        self.default_table_name = "nexmark_data"

        if not schema_json:
            raise ValueError("Schema is mandatory. Please provide a valid schema.")

        self.predefined_schema = self._parse_schema(schema_json)
        print(self.predefined_schema)

        self.stateful = stateful
        self.column_filter = column_filter
        self.window_size = window_size * 1e3 # convert to ms
        self.window_slide = window_slide * 1e3 # convert to ms

        if not self.stateful and (self.window_size > 0 or self.window_slide > 0):
            raise ValueError("Window parameters require --stateful mode.")

        if self.window_size == 0 and self.window_slide > 0:
            raise ValueError("Non-zero window-slide requires non-zero window-size.")

        self.person_buffer = []
        self.auction_buffer = []

        self.active_sources = {}
        self.window_buffer = {}

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


    def _apply_column_filter(self, stream_name: str, batch:pa.RecordBatch) -> pa.RecordBatch:
        cols = self.column_filter.get(stream_name)
        if not cols:
            return batch

        # Check for invalid columns
        existing = [c for c in cols if c in batch.schema.names]
        if not existing:
            return batch

        filtered = pa.Table.from_batches([batch]).select(existing)
        return filtered.to_batches()[0]


    def _assign_windows_to_timestamp(self, timestamp:int) -> list[int]:
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


    def _flush_window(self, batches: list[pa.RecordBatch], forward_writer):
        if not batches:
            return

        ctx = SessionContext()
        ctx.register_record_batches("window_table", partitions=[batches])

        df = ctx.sql(self.query)

        for batch in df.collect():
            forward_writer.write_batch(batch)

        ctx.deregister_table("window_table")


    def _collect_windows_to_flush(self, forward_writer):
        to_flush = []

        with self.lock:
            active_watermarks = [src["watermark"] for src in self.active_sources.values() if not src["is_finished"]]
            if not active_watermarks:
                # All sources are finished; flush all remaining windows
                window_starts = list(self.window_buffer.keys())
                for window_start in window_starts:
                    batches = self.window_buffer.pop(window_start)
                    to_flush.append(batches)
            else:
                global_watermark = min(active_watermarks)

                window_starts = list(self.window_buffer.keys())
                for window_start in window_starts:
                    window_end = window_start + self.window_size
                    if window_end <= global_watermark:
                        batches = self.window_buffer.pop(window_start)
                        to_flush.append(batches)
        for batches in to_flush:
            self._flush_window(batches, forward_writer)


    def process_default(self, reader, forward_writer):
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

        print(
            f'WM_LOG= {{"received_bytes": {total_bytes}, "start_time": {start},'
            f' "duration": {duration}, "MBps": {mbps:.2f}, "Gbps": {gbps:.4f}}}'
        )
        print("  receiving: ", sum(cost_break_down["receiving"]))
        print("  querying: ", sum(cost_break_down["querying"]))
        print("  sending: ", sum(cost_break_down["sending"]))
        print("forward_times = ", forwarding_times)


    def process_join(self, descriptor, reader, forward_writer):
        ctx = SessionContext()
        stream_type = descriptor.path[0].decode("utf-8")  if len(descriptor.path) > 0 else None

        for chunk in reader:
            batch = chunk.data
            if stream_type:
                batch = self._apply_column_filter(stream_type, batch)

            if stream_type == "person":
                self.person_buffer.append(batch)
                ctx.register_record_batches("person_table", [[batch]])
                if self.auction_buffer:
                    ctx.register_record_batches("auction_table", [self.auction_buffer])

            elif stream_type == "auction":
                self.auction_buffer.append(batch)
                ctx.register_record_batches("auction_table", [[batch]])
                if self.person_buffer:
                    ctx.register_record_batches("person_table", [self.person_buffer])

            else:
                continue

            if ctx.table_exist("person_table") and ctx.table_exist("auction_table"):
                result_df = ctx.sql(self.query)

                for result_batch in result_df.collect():
                    forward_writer.write_batch(result_batch)

            ctx.deregister_table("person_table")
            ctx.deregister_table("auction_table")

        forward_writer.done_writing()


    def process_windowed(self, descriptor, reader, forward_writer):
        source_id = descriptor.path[1].decode("utf-8")
        with self.lock:
            self.active_sources[source_id] = {"watermark": 0, "is_finished": False}
        stream_type = descriptor.path[0].decode("utf-8")  if len(descriptor.path) > 0 else None

        for chunk in reader:
            batch = chunk.data
            if stream_type:
                batch = self._apply_column_filter(stream_type, batch)

            bytes_meta = chunk.app_metadata.to_pybytes()
            watermark = int(bytes_meta.decode("utf-8"))

            timestamps = batch.column("date_time").cast(pa.int64())
            timestamps_list = timestamps.to_pylist()

            to_append = {}
            current_windows = None
            start_idx = 0

            for idx, ts in enumerate(timestamps_list):
                event_windows = self._assign_windows_to_timestamp(ts)

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

            with self.lock:
                self.active_sources[source_id]["watermark"] = watermark
                for win_start, sub_batches in to_append.items():
                    self.window_buffer.setdefault(win_start, []).extend(sub_batches)

            self._collect_windows_to_flush(forward_writer)

        with self.lock:
            self.active_sources[source_id]["is_finished"] = True

        self._collect_windows_to_flush(forward_writer)


    def do_put(self, context, descriptor, reader, writer):
        forward_writer, _ = self.forwarding_client.do_put(
            pa.flight.FlightDescriptor.for_path(self.query or self.default_table_name),
            self.predefined_schema,
        )

        if not self.stateful:
            self.process_default(reader, forward_writer)

        elif self.window_size == 0:
            self.process_join(descriptor, reader, forward_writer)

        else:
            self.process_windowed(descriptor,reader, forward_writer)


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
        "--stateful",
        action="store_true",
        help="Set to keep state of the incoming streams",
    )
    parser.add_argument(
        "--filter",
        type=str,
        help="JSON mapping of stream name -> list of columns to keep, "
             'e.g. \'{"person": ["id", "state"], "auction": ["id", "category"]}\''
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
    print(f" Stateful       : {args.stateful}")
    print(f" Filter         : {args.filter}")
    print(f" Window Size    : {args.window_size}")
    print(f" Window Slide   : {args.window_slide}")
    print("=" * 40 + "\n")

    port = args.port
    forward_node = args.forward_node
    sql_query = args.query
    schema_json = args.query_result_schema
    stateful = args.stateful
    window_size = args.window_size
    window_slide = args.window_slide

    if args.filter:
        try:
            column_filter = json.loads(args.filter)
            if not isinstance(column_filter, dict):
                raise ValueError("Parsed filter must be a JSON object.")
        except Exception as e:
            raise ValueError(f"Invalid --filter JSON: {e}")
    else:
        column_filter = None

    processing_node = ProcessingNode(
        f"grpc://0.0.0.0:{port}", forward_node, sql_query, schema_json, stateful, column_filter, window_size, window_slide
    )
    print(f"WoolMilk processing node running on port {port}")
    processing_node.serve()
