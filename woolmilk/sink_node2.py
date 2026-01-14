"""
    --- Sink Node Implementation ---
    Collects Data from Processing Node (via PUSH)
"""
import argparse
import os
import threading
import time
from queue import Queue, Empty

import pyarrow as pa
import pyarrow.flight as pf
import pyarrow.parquet as pq

from tools.logger import LogService, LogType
from tools.monitor import MonitorService

SHUTDOWN_FLAG: bool = False

class SinkNode(pf.FlightServerBase):
    def __init__(self, port: int, logger: LogService, advertised_host: str, queue_size: int,
                 result_folder=None, monitor: MonitorService=None):
        super().__init__(f"grpc://0.0.0.0:{port}")
        self.result_folder = result_folder
        if result_folder:
            os.makedirs(result_folder, exist_ok=True)
        self.file_counter = 0
        self.file_counter_lock = threading.Lock()

        self.logger = logger
        self.queue_size = queue_size

        self.monitor = monitor
        if self.monitor:
            self.monitor.connect_monitor(url=f"{advertised_host}:{port}")



    def do_put(self, context, descriptor, reader, writer):
        q: Queue[pa.RecordBatch | None] | None = None
        wt: threading.Thread | None = None
        local_id: int = 0


        if self.result_folder:
            with self.file_counter_lock:
                local_id = self.file_counter
                self.file_counter += 1

            q = Queue(self.queue_size)
            wt = threading.Thread(target=self.write_worker, args=(q, local_id), daemon=True)
            wt.start()

        total_bytes = 0
        start = time.time()
        it = iter(reader)
        while True:
            try:
                chunk = next(it)
            except StopIteration:
                break

            batch: pa.RecordBatch = chunk.data
            total_bytes += batch.nbytes

            if q is not None:
                q.put(batch)

        if q is not None:
            q.put(None)
        end = time.time()
        duration = end - start

        mbps = total_bytes / (duration * 1000**2) if duration > 0 else float("inf")
        gbps = (total_bytes * 8) / (duration * 1000**3) if duration > 0 else float("inf")
        self.logger.log(f'Receive: {{"received_bytes":{total_bytes},"duration":{duration:.6f},"MBps":{mbps:.2f},"Gbps":{gbps:.4f}, "file_id":{local_id}}}', LogType.INFO)

        if wt is not None:
            wt.join()

    def write_worker(self, queue: Queue, local_id: int):
        global SHUTDOWN_FLAG

        path = f"{self.result_folder}/{local_id}.parquet"
        writer: pq.ParquetWriter | None = None

        bytes_written = 0
        write_total_start = time.time()

        try:
            while not SHUTDOWN_FLAG:
                try:
                    batch: pa.RecordBatch = queue.get(timeout=0.5)
                except Empty:
                    continue
                if batch is None:
                    break

                if writer is None:
                    writer = pq.ParquetWriter(path, schema=batch.schema, compression="snappy")

                t0 = time.time()
                writer.write_batch(batch)
                t1 = time.time()

                b = batch.nbytes
                bytes_written += b
                dur = t1 - t0
                mbps = b / (dur * 1000**2) if dur > 0 else float("inf")
                self.logger.log(f'Write={{"write_batch_bytes":{b},"write_batch_s":{dur:.6f},"write_batch_MBps":{mbps:.2f},"file_id":{local_id}}}', LogType.DEBUG)

        finally:
            if writer is not None:
                writer.close()
            write_total_end = time.time()
            total_dur = write_total_end - write_total_start
            total_mbps = bytes_written / (total_dur * 1000**2) if total_dur > 0 else float("inf")
            self.logger.log(f'Write={{"write_total_bytes":{bytes_written},"write_total_s":{total_dur:.6f},"write_total_MBps":{total_mbps:.2f},"file_id":{local_id}}}', LogType.INFO)

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
        "--log-level", type=LogType, default=LogType.INFO, help="Log level"
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
    print(f" Advertised Host            : {args.advertised_host}")
    print(f" Port                       : {args.port}")
    print(f" Result Folder              : {args.result_folder}")
    print(f" Log-Level                  : {args.log_level}")
    print(f" Monitor Url                : {args.monitor_url}")
    print(f" Queue Max Size             : {args.queue_maxsize}")
    print("=" * 40 + "\n")
    return args

if __name__ == "__main__":
    args = parse_arguments()

    logger: LogService = LogService(args.log_level, log_max=1000)


    monitorService: MonitorService | None = None
    if args.monitor_url:
        monitorService = MonitorService(logger, monitor_url=f"grpc://{args.monitor_url}")

    sinkNode: SinkNode = SinkNode(args.port, logger, args.advertised_host, args.queue_maxsize,
                                  f"{args.result_folder}/{args.port}", monitorService)

    print(f"WoolMilk sink node running at port {args.port}")
    try:
        sinkNode.serve()
    except KeyboardInterrupt:
        pass

    if sinkNode.monitor and sinkNode.monitor.monitor_connected:
        sinkNode.monitor.disconnect_monitor(f"{args.advertised_host}:{args.port}")
    print("Shutting down Sink Node.")
    sinkNode.shutdown()
    exit(0)
