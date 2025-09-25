import argparse
import json
import threading
import time

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.flight
from datafusion import SessionContext


class ProcessingNode(pa.flight.FlightServerBase):
    def __init__(self, location, forward_node, sql_query, schema_json, enable_windowing, window_size):
        super().__init__(location)
        self.forwarding_client = pa.flight.FlightClient(f"grpc://{forward_node}")
        self.query = sql_query
        self.default_table_name = "nexmark_data"

        if not schema_json:
            raise ValueError("Schema is mandatory. Please provide a valid schema.")

        self.predefined_schema = self._parse_schema(schema_json)
        print(self.predefined_schema)

        self.state = {}
        self.enable_windowing = enable_windowing
        self.window_size = window_size * 1000

        if self.enable_windowing:
            self.lock = threading.Lock()
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
            now_ms = time.time() * 1000
            with self.lock:
                for window_start, batches in list(self.state.items()):
                    if now_ms >= window_start + self.window_size:
                        to_flush.append(self.state.pop(window_start))
            for batches in to_flush:
                self.flush_window(batches)
            time.sleep(0.1)

    def do_put(self, context, descriptor, reader, writer):
        if self.enable_windowing:
            for chunk in reader:
                batch = chunk.data

                # Get timestamps (Unix timestamps in ms)
                timestamps = batch.column("date_time").cast(pa.int64())
                window_size_scalar = pa.scalar(self.window_size, pa.int64())
                # Get window start timestamps by rounding down to nearest window size multiple
                window_starts = pc.multiply(pc.floor(pc.divide(timestamps, window_size_scalar)), window_size_scalar)

                # Split the batch into contiguous slices per window using run-end encoding
                ree = pc.run_end_encode(window_starts)
                run_ends = ree.run_ends.to_pylist()
                run_values = ree.values.to_pylist()

                start = 0
                for end, w_start_val in zip(run_ends, run_values):
                    end = int(end)
                    w_start_val = int(w_start_val)
                    sub_batch = batch.slice(start, end - start)
                    with self.lock:
                        self.state.setdefault(w_start_val, []).append(sub_batch)
                    start = end

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
    args = parser.parse_args()

    print("\n" + "=" * 40)
    print(" WoolMilk Processing Node Parameters")
    print("=" * 40)
    print(f" Port           : {args.port}")
    print(f" Forward Node   : {args.forward_node}")
    print(f" SQL Query      : {args.query}")
    print(f" Schema         : {args.query_result_schema}")
    print("=" * 40 + "\n")

    port = args.port
    forward_node = args.forward_node
    sql_query = args.query
    schema_json = args.query_result_schema
    enable_windowing = args.enable_windowing
    window_size = args.window_size

    processing_node = ProcessingNode(
        f"grpc://0.0.0.0:{port}", forward_node, sql_query, schema_json, enable_windowing, window_size
    )
    print(f"WoolMilk processing node running on port {port}")
    processing_node.serve()
