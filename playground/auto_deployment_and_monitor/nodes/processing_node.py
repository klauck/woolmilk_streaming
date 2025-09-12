"""
    ---- Processing Node Implementation ----
    Collects Data from Source Node (via PUSH) and pushed processed Data to Sink_Node
"""
import argparse
import json
import threading
import time
import pyarrow as pa

from datafusion import SessionContext
from pyarrow import flight

from tools.converters import str2bool
from tools.metrics import MonitorService, UNDEFINED_MONITOR, HealthResult, LogObject, LogType, MetricConfig, LIMIT


class ProcessingNode(flight.FlightServerBase):
    def __init__(self, location,advertised_host: str, clients: list[flight.FlightClient], monitor: MonitorService = None, sql_query: str = None, schema_json: str = None):
        super().__init__(f"grpc://{location}")
        self.clients = clients
        self.monitor = monitor
        self.query = sql_query
        self.default_table_name = "nexmark_data"

        if self.monitor:
            self.monitor.connect_monitor(advertised_host)


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
        t = action.type

        if t == "health":
            if self.monitor is None:
                raise flight.FlightServerError(UNDEFINED_MONITOR)
            with self.monitor.lock:
                payload: HealthResult = self.monitor.check_health()
                yield flight.Result(json.dumps(payload.to_dict()).encode("utf-8"))
        elif t == "logs" or t == "all_logs":
            if self.monitor is None:
                raise flight.FlightServerError(UNDEFINED_MONITOR)
            with self.monitor.lock:
                logs: list[LogObject] = self.monitor.parse_logs(t == "all_logs")
                payload: list[dict[str, str]] = [log.to_dict() for log in logs]
                yield flight.Result(json.dumps(payload).encode("utf-8"))
        elif t == "shutdown":
            if self.monitor is None:
                raise flight.FlightUnauthorizedError(UNDEFINED_MONITOR)

            def shutdown_server():
                time.sleep(0.1)
                self.shutdown()

            threading.Thread(target=shutdown_server, daemon=True).start()
            yield flight.Result(b"Server shutting down")
        else:
            if self.monitor:
                self.monitor.logs.append(LogObject(
                    f"SinkNode::do_action::Unknown action call '{action.type}'",
                    LogType.WARN))
            raise flight.FlightServerError(f"Unknown action: '{action.type}'")


    def handle_batch(self,reader, client: flight.FlightClient):
        ctx = SessionContext()
        forward_writer, _ = client.do_put(
            pa.flight.FlightDescriptor.for_path(self.default_table_name),
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

    def do_put(self, context, descriptor, reader, writer):
        for client in self.clients:
            self.handle_batch(reader, client)




if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="WoolMilk Processing Node")
    parser.add_argument(
        "--port",
        type=int,
        default=8010,
        help="Port to run the WoolMilk processing node",
    )
    parser.add_argument(
        "--forward-nodes",
        nargs="+",
        type=str,
        default=["localhost:8001"],
        help="Addresses of the node to forward data to (host:port)",
    )
    parser.add_argument(
        "--query",
        type=str,
        default="SELECT id FROM nexmark_data",
        help="SQL query to run on incoming batches",
    )
    parser.add_argument(
        "--query-result-schema",
        type=str,
        default='{"fields": [{"name": "id", "type": "int64"}]}',
        help="JSON schema definition for the data (required)",
    )
    parser.add_argument(
        "--monitor", type=str2bool, default=True, help="Allow Monitor?"
    )

    parser.add_argument(
        "--monitor_url", type=str, default="localhost:8000", help="Monitor URL"
    )

    parser.add_argument(
        "--advertised-host", type=str, default="localhost", help="URL reachable from Outside"
    )

    parser.add_argument(
        "--metric_cpu_warn", type=float, default=70.0, help="CPU warning metric"
    )
    parser.add_argument(
        "--metric_mem_warn", type=float, default=70.0, help="Memory warning metric"
    )

    parser.add_argument(
        "--metric_cpu_critical", type=float, default=85.0, help="CPU critical metric"
    )

    parser.add_argument(
        "--metric_mem_critical", type=float, default=80.0, help="Memory critical metric"
    )

    parser.add_argument(
        "--log-level", type=LogType, default=LogType.WARN, help="Log level"
    )

    args = parser.parse_args()

    print("\n" + "=" * 40)
    print(" WoolMilk Processing Node Parameters")
    print("=" * 40)
    print(f" Port           : {args.port}")
    if args.monitor:
        print(f" Monitor URL    : grpc://{args.monitor_url}")
    print(f" Forward Nodes  : {args.forward_nodes}")
    print(f" SQL Query      : {args.query}")
    print(" Schema         : Provided and parsed successfully")
    print("=" * 40 + "\n")

    port = args.port
    forward_nodes: list[flight.FlightClient]= [flight.FlightClient(f"grpc://{forward_node}") for forward_node in args.forward_nodes]

    sql_query = args.query
    schema_json = args.query_result_schema


    monitorService: MonitorService | None = None
    if args.monitor:
        monitorService = MonitorService(MetricConfig(LIMIT(args.metric_cpu_warn, args.metric_cpu_critical),
                                           LIMIT(args.metric_mem_warn, args.metric_mem_critical)),
                                        f"grpc://{args.monitor_url}")

    server = ProcessingNode(
        f"0.0.0.0:{port}", f"{args.advertised_host}:{port}", forward_nodes, monitorService, sql_query, schema_json
    )
    print(f"WoolMilk processing node running on port {port}")

    try:
        server.serve()
    except KeyboardInterrupt:
        pass
    if args.monitor and server.monitor.monitor_connected:
        server.monitor.disconnect_monitor(f"{args.advertised_host}:{port}")
    print("Shutting down Processing Node.")
    server.shutdown()
    exit(0)
