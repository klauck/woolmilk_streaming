import argparse
import json
import threading
import time

import pyarrow as pa
import pyarrow.flight

from queue import Queue
from datafusion import SessionContext
from pyarrow import flight, RecordBatch

from tools.metrics import (str2bool, LogType, MonitorService, LIMIT, MetricConfig, UNDEFINED_MONITOR,
                           HealthResult, LogObject, RawProcessingMetric, RawTransferMetric)


class ForwardClient:
    def __init__(self, url: str):
        self.url = url
        self.client = flight.FlightClient(f"grpc://{url}")

class ProcessingNode(pa.flight.FlightServerBase):
    def __init__(self, location, advertised_host: str, forward_node: str, sql_query, schema_json, monitor: MonitorService = None):
        super().__init__(location)
        self.forwarding_clients: list[ForwardClient] = [ForwardClient(forward_node)]

        self.query = sql_query
        self.default_table_name = "nexmark_data"

        self.monitor = monitor
        if self.monitor:
            self.monitor.connect_monitor(advertised_host, [forward_node], sql_query)

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

        elif t == "metrics":
            if self.monitor is None:
                raise flight.FlightServerError(UNDEFINED_MONITOR)
            with self.monitor.lock:
                metrics = self.monitor.parse_metrics()
                yield flight.Result(json.dumps(metrics).encode("utf-8"))

        elif t == "logs" or t == "all_logs":
            #TODO
            if self.monitor is None:
                raise flight.FlightServerError(UNDEFINED_MONITOR)
            with self.monitor.lock:
                logs: list[LogObject] = self.monitor.parse_logs(t == "all_logs")
                payload: list[dict[str, str]] = [log.to_dict() for log in logs]
                yield flight.Result(json.dumps(payload).encode("utf-8"))

        elif t == "shutdown":
            #TODO
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

    def do_put(self, context: flight.ServerCallContext, descriptor: flight.FlightDescriptor,
               reader: flight.MetadataRecordBatchReader, writer: flight.MetadataRecordBatchWriter):

        source_id: str = context.peer()

        #### Forward Clients run as Threads for non blocking the processing ####
        fw_queues: list[Queue] = []
        fw_workers: list[threading.Thread] = []

        for fw_client in self.forwarding_clients:
            fw_writer, _ = fw_client.client.do_put(
                flight.FlightDescriptor.for_path(self.query or self.default_table_name),
                self.predefined_schema
            )

            q = Queue()
            fw_queues.append(q)

            t = threading.Thread(
                target=self.forward_worker,
                args=(fw_writer, q, source_id, fw_client.url),
                daemon=True
            )
            t.start()
            fw_workers.append(t)



        #### Process Batch first (Metrices are recorder here first) ###

        self.handle_batch(
            reader,
            fw_queues,
            source_id,
        )

        ### Clean all threads and Queues
        for q in fw_queues:
            q.put(None)

        for t in fw_workers:
            t.join()


    def handle_batch(self, reader: flight.MetadataRecordBatchReader, fw_queues: list[Queue], source_id: str):
        """
            Processes given batch (and takes metrices of the processing) and stores to forward_queue
        """

        ctx = SessionContext()

        for chunk in reader:
            batch = chunk.data

            processing_start = time.time()
            ctx.register_record_batches(self.default_table_name, [[batch]])
            result_df = ctx.sql(self.query)
            result_batches = result_df.collect()
            ctx.deregister_table(self.default_table_name)
            processing_end = time.time()

            self.store_processing_metric(
                source_id,
                processing_end, #timestamp
                processing_end - processing_start, #processing-time
                batch.nbytes #batch-size
            )

            for qu in fw_queues:
                qu.put(result_batches)



    def store_processing_metric(self, source: str, timestamp:float, duration:float, size: int):
        if self.monitor:
            with self.monitor.lock:
                print(f"Storing Processing Metric: {source}, {timestamp}, {duration}, {size}")
                self.monitor.raw_processing_metrics.append(RawProcessingMetric(
                    source,
                    duration,
                    size,
                    timestamp
                ))


    def forward_worker(self, writer: flight.FlightStreamWriter, queue: Queue[list[RecordBatch]], from_id: str, to_id: str):
        """
            Reads batches from the queue and forwards it to given client
        """
        while True:
            records: list[RecordBatch] = queue.get()
            if records is None:
                break

            send_start = time.time()
            bytes_sent: int = 0

            for rb in records:
                writer.write_batch(rb)
                bytes_sent += rb.nbytes

            send_end = time.time()
            self.store_forward_metric(from_id, to_id, send_end, send_end - send_start, bytes_sent)

        writer.done_writing()



    def store_forward_metric(self, source: str, client: str, timestamp: float, duration: float, size: int):
        if self.monitor:
            with self.monitor.lock:
                print(f"Storing Transfering Metric: {source}, {client}, {timestamp}, {duration}, {size}")
                self.monitor.raw_transfer_metrics.append(RawTransferMetric(
                    source,
                    client,
                    duration,
                    size,
                    timestamp
                ))


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
    print(f" Forward Node   : {args.forward_node}")
    print(f" SQL Query      : {args.query}")
    print(f" Schema         : {args.query_result_schema}")
    print("=" * 40 + "\n")

    port = args.port
    forward_node = args.forward_node
    sql_query = args.query
    schema_json = args.query_result_schema

    monitorService: MonitorService | None = None
    if args.monitor:
        monitorService = MonitorService(MetricConfig(LIMIT(args.metric_cpu_warn, args.metric_cpu_critical),
                                                     LIMIT(args.metric_mem_warn, args.metric_mem_critical)),
                                        f"grpc://{args.monitor_url}")

    processing_node = ProcessingNode(
        f"grpc://0.0.0.0:{port}", f"{args.advertised_host}:{port}", forward_node, sql_query, schema_json, monitorService
    )
    print(f"WoolMilk processing node running on port {port}")
    try:
        processing_node.serve()
    except KeyboardInterrupt:
        pass
    if args.monitor and processing_node.monitor.monitor_connected:
        processing_node.monitor.disconnect_monitor(f"{args.advertised_host}:{port}")
    print("Shutting down Processing Node.")
    processing_node.shutdown()
    exit(0)
