import argparse
import json
import queue
import threading
import time
from queue import Queue

import pyarrow as pa
import pyarrow.flight as pf
from datafusion import SessionContext

from tools.evaluation import OverheadEvaluation
from tools.metrics import HealthConfig, LIMIT, HealthResult, metrics_to_record_batch, Metric, MetricType
from tools.logger import LogType, LogService
from tools.monitor import MonitorService, NodeType

SHUTDOWN_FLAG = False
Logger: LogService | None = None
evaluation: OverheadEvaluation | None = None

class WriteWorker:
    def __init__(self, current_address: str, client_address: str, queue_size: int):
        self.client_address = client_address
        self.client = pf.FlightClient(f"grpc://{client_address}")
        self.writer: pf.FlightStreamWriter | None = None
        self.queue = Queue(queue_size)
        self.thread: threading.Thread | None = None


class ProcessingNode(pf.FlightServerBase):
    def __init__(self, port: int, advertised_host: str, forward_nodes: list[str], sql_query: str, monitor: MonitorService | None=None):
        super().__init__(f"grpc://0.0.0.0:{port}")
        Logger.log("Initializing Apache Flight Server for Processing Node", LogType.INFO)
        self.monitor = monitor
        if self.monitor:
            self.monitor.connect_monitor(url=f"{advertised_host}:{port}", node_type=NodeType.PROCESS, forward_url=forward_nodes, query=sql_query)

        self.current_address = f"{advertised_host}:{port}"
        self.query = sql_query
        self.forward_nodes = forward_nodes
        self.process_threads: list[threading.Thread] = []
        self.send_threads: list[threading.Thread] = []


    def shutdown(self):
        Logger.log("Shutting down Apache Flight Server", LogType.INFO)

        Logger.log("Closing all Processing and Sender Threads", LogType.DEBUG)
        for thread in self.process_threads:
            thread.join(timeout=3)

        for thread in self.send_threads:
            thread.join(timeout=3)

        super().shutdown()




    def do_action(self, context, action):
        Logger.log(f"Got Apache Request - Action [{action.type}]", LogType.DEBUG)
        if action.type == "logs":
            serialize = Logger.get_logs()
            yield pa.flight.Result(json.dumps(serialize).encode("utf-8"))

        elif action.type == "health":
            payload: HealthResult = self.monitor.check_health()
            yield pf.Result(json.dumps(payload.to_dict()).encode("utf-8"))

        elif action.type == "metrics":
            with self.monitor.metric_lock:
                metrics = []
                while not self.monitor.metric_queue.empty():
                    metrics.append(self.monitor.metric_queue.get())

            yield pa.flight.Result(metrics_to_record_batch(metrics).serialize().to_pybytes())


    def do_put(self, context, descriptor: pf.FlightDescriptor, reader, writer):
        client_url = descriptor.path[0].decode("utf-8") if descriptor.path[0] else "Unknown Client"
        Logger.log(f"Got a new Put Channel from Client {client_url}", LogType.INFO)

        process_queue = Queue()
        process_thread = threading.Thread(target=self.process_batches, args=(process_queue, client_url), daemon=True)
        process_thread.start()

        self.process_threads.append(process_thread)


        self.received_batch(reader, process_queue, client_url)

        process_queue.put(None)
        process_thread.join()
        self.process_threads.remove(process_thread)


    def received_batch(self, reader, queue, address):
        total_mbytes = 0
        total_duration_ms = 0
        it = iter(reader)

        while True:
            start_time = time.time_ns()
            try:
                chunk = next(it)
            except StopIteration:
                break

            batch: pa.RecordBatch = chunk.data

            if queue is not None:
                queue.put(batch)

            end_time = time.time_ns()

            duration_ns = end_time - start_time
            if self.monitor:
                self.monitor.metric_queue.put(Metric(address, MetricType.RECEIVE, duration_ns, batch.nbytes))

            #if evaluation:
            #    evaluation.add_event_metric(MetricType.RECEIVE, duration_ns, batch.nbytes, batch.num_rows)

            batch_mbytes = batch.nbytes / (10 ** 6)
            duration_ms = duration_ns / (10 ** 6)
            mbps = ((batch_mbytes / duration_ms) * 1000) if duration_ms > 0 else float("inf")
            Logger.log(
                f"[Receive Batch] Size {batch_mbytes:.2f}MB in {duration_ms:.2f}ms by {address} [{mbps:.2f} MB/s]",
                LogType.DEBUG)

            total_mbytes += batch_mbytes
            total_duration_ms += duration_ms

        avg_mbps = ((total_mbytes / total_duration_ms) * 1000) if total_duration_ms > 0 else float("inf")
        Logger.log(
            f"[Receive Batch] Total Size {total_mbytes:.2f}MB in {total_duration_ms:.2f}ms by {address} [{avg_mbps:.2f} MB/s]",
            LogType.INFO)

        if queue is not None:
            queue.put(None)

    def create_write_workers(self, client_url: str):
        Logger.log("Creating Sender Worker", LogType.INFO)
        sender_workers: list[WriteWorker] = []

        for node in self.forward_nodes:
            w = WriteWorker(self.current_address, node, 100)

            t = threading.Thread(target=self.send_batch, args=(w, client_url), daemon=True)
            w.thread = t
            t.start()
            sender_workers.append(w)
            self.send_threads.append(t)

        return sender_workers

    def process_batches(self, q: Queue[pa.RecordBatch], client_url):
        workers = self.create_write_workers(client_url)

        writer_index = 0

        total_mbytes = 0
        total_duration_ms = 0

        ctx = SessionContext()
        while not SHUTDOWN_FLAG:
            try:
                batch: pa.RecordBatch = q.get(timeout=3)
            except queue.ShutDown:
                break
            except queue.Empty:
                continue

            if batch is None:
                break


            batch_bytes = batch.nbytes
            start_time = time.time_ns()

            ctx.register_record_batches("nexmark_data", [[batch]])

            result_df = ctx.sql(self.query)


            time.sleep(0.025)        #<--- Fake Bottleneck :)

            result = result_df.collect()

            for rbatch in result:
                workers[writer_index].queue.put(rbatch)
                writer_index = (writer_index + 1) % len(workers)


            ctx.deregister_table("nexmark_data")

            end_time = time.time_ns()
            duration_ns = end_time - start_time

            if self.monitor:
                self.monitor.metric_queue.put(Metric(client_url, MetricType.PROCESS, duration_ns, batch.nbytes))

            #if evaluation:
            #    evaluation.add_event_metric(MetricType.PROCESS, duration_ns, batch.nbytes, batch.num_rows)

            batch_mbytes = batch_bytes / (10 ** 6)
            duration_ms = duration_ns / (10 ** 6)
            mbps = ((batch_mbytes / duration_ms) * 1000) if duration_ms > 0 else float("inf")
            Logger.log(
                f"[Process Batch] Process From {client_url} with Size {batch_mbytes:.2f} MB in {duration_ms:.2f}ms [{mbps:.2f} MB/s]",
                LogType.DEBUG)
            total_mbytes += batch_mbytes
            total_duration_ms += duration_ms


        avg_mbps = ((total_mbytes / total_duration_ms) * 1000) if total_duration_ms > 0 else float("inf")
        Logger.log(f"[Process Batch] Complete Process From {client_url} in {total_duration_ms:.2f}ms with size {total_mbytes:.2f} MB. [{avg_mbps:.2f}MB/s]", LogType.INFO)

        for worker in workers:
            worker.queue.put(None)
            worker.thread.join()
            self.send_threads.remove(worker.thread)

    def send_batch(self, worker: WriteWorker, client_url: str):
        total_mbytes = 0
        total_duration_ms = 0

        while not SHUTDOWN_FLAG:
            try:
                batch: pa.RecordBatch = worker.queue.get(timeout=3)
            except queue.ShutDown:
                break
            except queue.Empty:
                continue

            if batch is None:
                break

            if worker.writer is None:
                schema: pa.Schema = batch.schema
                flight_descriptor: pf.FlightDescriptor = pf.FlightDescriptor.for_path(self.current_address)
                worker.writer, _ = worker.client.do_put(flight_descriptor, schema)

            start_time = time.time_ns()
            worker.writer.write_batch(batch)
            end_time = time.time_ns()
            duration_ns = end_time - start_time

            if self.monitor:
                self.monitor.metric_queue.put(Metric(client_url, MetricType.SEND, duration_ns, batch.nbytes))

            #if evaluation:
            #    evaluation.add_event_metric(MetricType.SEND, duration_ns, batch.nbytes, batch.num_rows)

            batch_mbytes = batch.nbytes / (10 ** 6)
            duration_ms = duration_ns / (10 ** 6)
            mbps = ((batch_mbytes / duration_ms) * 1000) if duration_ms > 0 else float("inf")
            Logger.log(
                f"[Send Batch] Send processed data From {client_url} with Size {batch_mbytes:.2f} MB in {duration_ms:.2f}ms [{mbps:.2f} MB/s]",
                LogType.DEBUG)
            total_mbytes += batch_mbytes
            total_duration_ms += duration_ms

        avg_mbps = ((total_mbytes / total_duration_ms) * 1000) if total_duration_ms > 0 else float("inf")
        Logger.log(
            f"[Send Batch] Complete send of processed Data From {client_url} in {total_duration_ms:.2f}ms with size {total_mbytes:.2f} MB. [{avg_mbps:.2f}MB/s]",                LogType.INFO)
        worker.writer.done_writing()




def parse_arguments():
    parser = argparse.ArgumentParser(description="WoolMilk Processing Node")
    parser.add_argument(
        "--advertised-host", type=str, default="localhost", help="URL reachable from Outside"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8010,
        help="Port to run the WoolMilk processing node",
    )
    parser.add_argument(
        "--forward-nodes",
        help="Flight server address (host:port,host:port)",
        type=str,
        default="localhost:8020",
    )
    parser.add_argument(
        "--query",
        type=str,
        default="SELECT * FROM nexmark_data",
        help="SQL query to run on incoming batches",
    )
    parser.add_argument(
        "--log-save-level", type=LogType, default=LogType.INFO, help="Log level"
    )
    parser.add_argument(
        "--log-print-level", type=LogType, default=LogType.DEBUG, help="Log level"
    )
    parser.add_argument(
        "--monitor_url", type=str, default=None, help="Monitor URL"
    )
    parser.add_argument(
        "--metric_cpu_warn", type=float, default=60.0, help="CPU warning metric"
    )
    parser.add_argument(
        "--metric_mem_warn", type=float, default=60.0, help="Memory warning metric"
    )

    parser.add_argument(
        "--metric_cpu_critical", type=float, default=85.0, help="CPU critical metric"
    )

    parser.add_argument(
        "--metric_mem_critical", type=float, default=80.0, help="Memory critical metric"
    )
    args = parser.parse_args()
    print("\n" + "=" * 40)
    print(" WoolMilk Processing Node Parameters")
    print("=" * 40)
    print(f" Host                       : {args.advertised_host}")
    print(f" Port                       : {args.port}")
    print(f" Monitor                    : {args.monitor_url}")
    print(f" Forward Nodes              : {args.forward_nodes}")
    print(f" SQL Query                  : {args.query}")
    print("=" * 40 + "\n")
    return args

if __name__ == "__main__":
    #evaluation = OverheadEvaluation(interval_sec=1.0)
    #evaluation.start()
    args = parse_arguments()

    Logger = LogService(args.log_save_level, args.log_print_level)
    Logger.log("Starting Woolmilk Processing Node", LogType.INFO)

    healthConfig: HealthConfig = HealthConfig(LIMIT(args.metric_cpu_warn, args.metric_cpu_critical),
                                              LIMIT(args.metric_mem_warn, args.metric_mem_critical))
    monitor = None
    if args.monitor_url:
        monitor = MonitorService(Logger, monitor_url=f"grpc://{args.monitor_url}", health_config=healthConfig)

    nodes: list[str] = [n.strip() for n in args.forward_nodes.split(",") if n.strip()]

    processing_node = ProcessingNode(args.port, args.advertised_host, nodes, args.query, monitor)

    try:
        processing_node.serve()

    except KeyboardInterrupt:
        SHUTDOWN_FLAG = True

    finally:
        if processing_node.monitor and processing_node.monitor.monitor_connected:
            processing_node.monitor.disconnect_monitor(f"{args.advertised_host}:{args.port}")

        Logger.log("Shutting down WoolMilk Source Node", LogType.INFO)
        processing_node.shutdown()
        Logger.log("Shutdown Completed!", LogType.INFO)

        #evaluation.stop()
        #evaluation.create_file(f"load3_monitor_processing_{args.port}")