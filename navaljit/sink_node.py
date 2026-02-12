"""
    --- Sink Node Implementation ---
    Collects Data from Processing Node (via PUSH)
"""
import json
import os
import argparse
import threading
import time
from pathlib import Path
from queue import Queue, Empty

import pyarrow as pa
import pyarrow.flight as pf
import pyarrow.parquet as pq

from tools.monitor import MonitorService
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
        if result_folder:
            os.makedirs(result_folder, exist_ok=True)


        self.monitor = monitor
        if self.monitor:
            self.monitor.connect_monitor(url=f"{advertised_host}:{port}")

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


    def received_batch(self, reader: pf.MetadataRecordBatchReader, q: Queue[pa.RecordBatch | None] | None, address: str):
        total_received_mbytes = 0
        total_duration = 0
        it = iter(reader)

        while True:
            start_time = time.time()
            try:
                chunk = next(it)
            except StopIteration:
                break

            batch: pa.RecordBatch = chunk.data

            if q is not None:
                q.put(batch)

            end_time = time.time()


            batch_mbytes = batch.nbytes / (1024**2) #MB
            total_received_mbytes += batch_mbytes

            duration_s = (end_time - start_time)
            duration_ms = duration_s * 1000
            total_duration += duration_s
            mbps = batch_mbytes / duration_s if duration_s > 0 else float("inf")
            Logger.log(f"[Receive Batch] Size {batch_mbytes:.2f}MB in {duration_ms:.2f}ms by {address} [{mbps:.2f} MB/s]",
                       LogType.DEBUG)

        avg_mbps = total_received_mbytes / total_duration if total_duration > 0 else float("inf")
        Logger.log(f"[Receive Batch] Total Size {total_received_mbytes:.2f}MB in {(total_duration * 1000):.2f}ms by {address} [{avg_mbps:.2f} MB/s]", LogType.INFO)
        if q is not None:
            q.put(None)

    def close_threads(self):
        for thread in self.threads:
            thread.join(timeout=3)

    def write_parquet(self, queue: Queue[pa.RecordBatch | None], address: str):
        global SHUTDOWN_FLAG

        path = Path(f"{self.result_folder}/{address}/{time.time_ns()}.parquet")
        path.parent.mkdir(parents=True, exist_ok=True)

        writer: pq.ParquetWriter | None = None

        bytes_written = 0
        total_duration = 0

        try:
            while not SHUTDOWN_FLAG:
                try:
                    batch: pa.RecordBatch = queue.get(timeout=3)
                except Empty:
                    continue
                if batch is None:
                    break

                if writer is None:
                    writer = pq.ParquetWriter(path, schema=batch.schema, compression="snappy")

                t0 = time.time()
                writer.write_batch(batch)
                t1 = time.time()

                batch_mbytes = batch.nbytes / (1024**2)
                bytes_written += batch.nbytes
                duration_s = (t1 - t0)
                duration_ms = duration_s * 1000
                total_duration += duration_s
                mbps = batch_mbytes / duration_s if duration_s > 0 else float("inf")
                Logger.log(f"[Write Parquet] Write Batch of Size {batch_mbytes:.2f} MB in {duration_ms:.2f}ms into file {path} [{mbps:.2f} MB/s]", LogType.DEBUG)

        finally:
            mbytes_total = bytes_written / (1024 ** 2)
            avg_mbps = mbytes_total / total_duration if total_duration > 0 else float("inf")
            Logger.log(f"[Write Parquet] Complete write of file {path} in {(total_duration * 1000):.2f}ms with size {mbytes_total:.2f} MB. [{avg_mbps:.2f}MB/s]", LogType.INFO)
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

    monitorService: MonitorService | None = None
    if args.monitor_url:
        monitorService = MonitorService(Logger, monitor_url=f"grpc://{args.monitor_url}")

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


