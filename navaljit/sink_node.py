"""
    --- Sink Node Implementation ---
    Collects Data from Processing Node (via PUSH)
"""
import json
import os
import argparse
import queue
import threading
import time
from pathlib import Path
from queue import Queue

import pyarrow as pa
import pyarrow.flight as pf
import pyarrow.parquet as pq

from tools.metrics import HealthStatus, HealthConfig, LIMIT, HealthResult
from tools.metrics import Metric, MetricType, metrics_to_record_batch
from tools.monitor import MonitorService, NodeType
from tools.logger import LogService, LogType
Logger: LogService | None = None


SHUTDOWN_FLAG: bool = False



'''
    Apache Flight Server Thread
'''

class SinkNode(pf.FlightServerBase):
    def __init__(self, port: int, advertised_host: str, queue_size: int, result_folder: str | None = None, monitor: MonitorService | None=None ):
        super().__init__(f"grpc://localhost:{port}")
        self.queue_size = queue_size
        self.result_folder = result_folder
        self.threads: list[threading.Thread] = []
        self.status: HealthStatus = HealthStatus.OK
        if result_folder:
            os.makedirs(result_folder, exist_ok=True)


        self.monitor = monitor
        if self.monitor:
            self.monitor.connect_monitor(url=f"{advertised_host}:{port}", node_type=NodeType.SINK)

    def do_put(self, context, descriptor: pf.FlightDescriptor, reader, writer):
        q: Queue[pa.RecordBatch | None] | None = None
        worker_thread: threading.Thread | None = None

        path_bytes = descriptor.path[0]
        address = path_bytes.decode("utf-8")

        if self.result_folder:
            q = Queue(self.queue_size)
            worker_thread = threading.Thread(target=self.write_parquet, args=(q,address), daemon=True)
            worker_thread.start()
            self.threads.append(worker_thread)


        self.received_batch(reader, q, address)

        if worker_thread is not None:
            worker_thread.join()

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

    def received_batch(self, reader: pf.MetadataRecordBatchReader, q: Queue[pa.RecordBatch | None] | None, address: str):
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

            if q is not None:
                q.put(batch)

            end_time = time.time_ns()

            duration_ns = end_time - start_time
            if self.monitor:
                self.monitor.metric_queue.put(Metric(address, MetricType.RECEIVE, duration_ns, batch.nbytes))

            batch_mbytes = batch.nbytes / (10**6)
            duration_ms = duration_ns / (10**6)
            mbps = ((batch_mbytes / duration_ms) * 1000) if duration_ms > 0 else float("inf")
            Logger.log(f"[Receive Batch] Size {batch_mbytes:.2f}MB in {duration_ms:.2f}ms by {address} [{mbps:.2f} MB/s]",
                       LogType.DEBUG)

            total_mbytes += batch_mbytes
            total_duration_ms += duration_ms


        avg_mbps = ((total_mbytes / total_duration_ms) * 1000) if total_duration_ms > 0 else float("inf")
        Logger.log(f"[Receive Batch] Total Size {total_mbytes:.2f}MB in {total_duration_ms:.2f}ms by {address} [{avg_mbps:.2f} MB/s]", LogType.INFO)

        if q is not None:
            q.put(None)

    def close_threads(self):
        for thread in self.threads:
            thread.join(timeout=3)

    def write_parquet(self, batch_queue: Queue[pa.RecordBatch | None], address: str):
        global SHUTDOWN_FLAG

        path = Path(f"{self.result_folder}/{address.replace(':', '-')}/{time.time_ns()}.parquet")
        path.parent.mkdir(parents=True, exist_ok=True)

        writer: pq.ParquetWriter | None = None

        total_mbytes = 0
        total_duration_ms = 0

        try:
            while not SHUTDOWN_FLAG:
                try:
                    batch: pa.RecordBatch = batch_queue.get(timeout=3)
                except queue.ShutDown:
                    break
                except queue.Empty:
                    continue
                if batch is None:
                    break

                if writer is None:
                    writer = pq.ParquetWriter(path, schema=batch.schema, compression="snappy")

                start_time = time.time_ns()
                writer.write_batch(batch)
                end_time = time.time_ns()

                duration_ns = end_time - start_time

                if self.monitor:
                    self.monitor.metric_queue.put(Metric(str(path), MetricType.SEND, duration_ns, batch.nbytes))

                batch_mbytes = batch.nbytes / (10 ** 6)
                duration_ms = duration_ns / (10 ** 6)
                mbps = ((batch_mbytes / duration_ms) * 1000) if duration_ms > 0 else float("inf")
                Logger.log(f"[Write Parquet] Write Batch of Size {batch_mbytes:.2f} MB in {duration_ms:.2f}ms into file {path} [{mbps:.2f} MB/s]", LogType.DEBUG)
                total_mbytes += batch_mbytes
                total_duration_ms += duration_ms

        finally:
            avg_mbps = ((total_mbytes / total_duration_ms) * 1000) if total_duration_ms > 0 else float("inf")
            Logger.log(f"[Write Parquet] Complete write of file {path} in {total_duration_ms:.2f}ms with size {total_mbytes:.2f} MB. [{avg_mbps:.2f}MB/s]", LogType.INFO)

            if writer is not None:
                writer.close()


'''
    Main - Thread
'''

def parse_arguments():
    parser = argparse.ArgumentParser(description='Woolmilk Sink node')
    parser.add_argument(
        "--advertised-host", type=str, default="localhost", help="URL reachable from Outside"
    )
    parser.add_argument(
        "--port", type=int, default=8020, help="Port to run the WoolMilk sink node"
    )

    parser.add_argument(
        "--result-folder", type=str, default="results", help="Folder to store results"
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
        "--queue-maxsize", type=int, default=100, help="Backpressure queue size for each Batch"
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

    args: argparse.Namespace = parser.parse_args()

    print("\n" + "=" * 40)
    print(" WoolMilk Sink Node Parameters")
    print("=" * 40)
    print(f" Host                       : {args.advertised_host}")
    print(f" Port                       : {args.port}")
    print(f" Monitor                    : {args.monitor_url}")
    print(f" Result Folder              : {args.result_folder}")
    print(f" Queue Max Size             : {args.queue_maxsize}")
    print("=" * 40 + "\n")
    return args


if __name__ == "__main__":
    args = parse_arguments()

    Logger = LogService(args.log_save_level, args.log_print_level)
    Logger.log("Starting WoolMilk Sink Node", LogType.INFO)

    healthConfig: HealthConfig = HealthConfig(LIMIT(args.metric_cpu_warn, args.metric_cpu_critical),
                                              LIMIT(args.metric_mem_warn, args.metric_mem_critical))

    monitorService: MonitorService | None = None
    if args.monitor_url:
        monitorService = MonitorService(Logger, monitor_url=f"grpc://{args.monitor_url}", health_config=healthConfig)

    sinkNode: SinkNode = SinkNode(args.port,args.advertised_host, args.queue_maxsize, f"{args.result_folder}/{args.port}", monitorService)

    try:
        sinkNode.serve()
    except KeyboardInterrupt:
        SHUTDOWN_FLAG = True

    finally:
        if sinkNode.monitor and sinkNode.monitor.monitor_connected:
            sinkNode.monitor.disconnect_monitor(f"{args.advertised_host}:{args.port}")
        Logger.log("Shutting down WoolMilk Sink Node", LogType.INFO)
        sinkNode.close_threads()
        sinkNode.shutdown()
        Logger.log("Bye!", LogType.INFO)


