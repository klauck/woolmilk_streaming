import argparse
import json
import threading
import time

import pyarrow as pa
import pyarrow.flight
from datafusion import SessionContext


class ProcessingNode(pa.flight.FlightServerBase):
    def __init__(self, location, forward_node, sql_query, schema_json):
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
        if action.type == "get_logs":
            with self.logs_lock:
                logs = self.logs
            yield pyarrow.flight.Result(json.dumps(logs).encode("utf-8"))
        elif action.type == "delete_logs":
            with self.logs_lock:
                self.logs = []
        else:
            raise NotImplementedError(f"Unknown action: {action.type}")

    def do_put(self, context, descriptor, reader, writer):
        ctx = SessionContext()

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
        gbps = (total_bytes * 8) / (duration * 1000**3)
        mbps = total_bytes / (duration * 1000**2)

        log = {
            "experiment_id": experiment_id,
            "iteration_id": iteration_id,
            "source_node_id": source_node_id,
            "thread_id": thread_id,
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

    processing_node = ProcessingNode(
        f"grpc://0.0.0.0:{port}", forward_node, sql_query, schema_json
    )
    print(f"WoolMilk processing node running on port {port}")
    processing_node.serve()
