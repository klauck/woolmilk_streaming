import argparse
import json
import queue
import threading
import time
from queue import Queue
from typing import Iterator

import pyarrow as pa
import pyarrow.flight as pf
import pyarrow.parquet as pq

from tools.monitor import MonitorService
from tools.logger import LogService, LogType
Logger: LogService | None = None


SHUTDOWN_FLAG = False

'''
    Apache Flight Server Thread
'''
class SourceNode(pa.flight.FlightServerBase):
    def __init__(self, port: int, advertised_host: str, monitor: MonitorService | None=None):
        super().__init__(f"grpc://0.0.0.0:{port}")
        Logger.log("Initializing Apache Flight Server for Source Node", LogType.INFO)

        self.monitor = monitor
        if self.monitor:
            self.monitor.connect_monitor(url=f"{advertised_host}:{port}")


    def do_action(self, context, action):
        Logger.log(f"Got Apache Request - Action [{action.type}]", LogType.DEBUG)
        if action.type == "logs":
            serialize = Logger.get_logs()
            yield pa.flight.Result(json.dumps(serialize).encode("utf-8"))


'''
    Sender Thread
'''

class WriteWorker:
    def __init__(self, address: str, queue_size: int, schema: pa.Schema):
        self.address = address
        self.client = pf.FlightClient(f"grpc://{address}")
        flight_descriptor: pf.FlightDescriptor = pf.FlightDescriptor.for_path(f"source-{address.replace(':', '-')}")
        self.writer, _ = self.client.do_put(flight_descriptor, schema)
        self.queue = Queue(queue_size)
        self.thread: threading.Thread | None = None

def send_batch(worker: WriteWorker):
    global SHUTDOWN_FLAG

    current_second = int(time.time())
    batches_sent_this_second = 0
    batches_total = 0
    bytes_sent_this_second = 0
    mbytes_total = 0

    send_duration = 0.0
    send_duration_total = 0.0

    while not SHUTDOWN_FLAG:
        try:
            batch: pa.RecordBatch = worker.queue.get()
        except queue.ShutDown:
            break

        if batch is None:
            break

        now = int(time.time())
        if now != current_second:
            mbps = (bytes_sent_this_second / (1000 ** 2)) #SI Standard
            avg_duration = int(send_duration / batches_sent_this_second) if batches_sent_this_second > 0 else float("inf")
            Logger.log(f"[Send Batch] Worker ({worker.address}) sent {batches_sent_this_second} Batches ({mbps:.2f} MB/s) on second {current_second}, the average duration is {avg_duration}ms with total duration {send_duration:.2f} ms.", LogType.DEBUG)
            current_second = now
            batches_total += batches_sent_this_second
            batches_sent_this_second = 0
            bytes_sent_this_second = 0
            send_duration_total += send_duration
            send_duration = 0

        t0 = time.time_ns()

        try:
            worker.writer.write_batch(batch)

        except Exception as e:
            Logger.log(f"[Send Batch] Worker ({worker.address}) failed with exception {e}", LogType.ERROR)
            SHUTDOWN_FLAG = True
            return

        t1 = time.time_ns()

        batches_sent_this_second += 1
        bytes_sent_this_second += batch.nbytes
        send_duration += (t1 - t0) / (1000**2) #ms
        mbytes_total += (batch.nbytes / (1024**2) )

    try:
        worker.writer.done_writing()
        avg_mbps = mbytes_total / (send_duration_total / 1000) if send_duration_total > 0 else float("inf")
        Logger.log(
            f"[Send Batch] Sent {batches_total} Batches of total Size {mbytes_total:.2f}MB in {send_duration_total:.2f}ms by {worker.address} [{avg_mbps:.2f} MB/s]", LogType.INFO)
    except Exception:
        pass



'''
    Producer / Main - Thread
'''

def add_timestamp(batch: pa.RecordBatch) -> pa.RecordBatch:
    now = int(time.time())
    timestamps = pa.array([now] * batch.num_rows, type=pa.int64())
    return batch.append_column("timestamp", timestamps)


def parse_parquet_file(parquet_file: pq.ParquetFile, overall_batches: int, tuple_per_batch: int) -> Iterator[
    pa.RecordBatch]:
    current_batch_count = 0
    while overall_batches == -1 or current_batch_count < overall_batches:
        for batch in parquet_file.iter_batches(batch_size=tuple_per_batch):
            if overall_batches != -1:
                if current_batch_count >= overall_batches:
                    return
                current_batch_count += 1
            yield add_timestamp(batch)


def distribute_batches(gen: Iterator[pa.RecordBatch], workers: list[WriteWorker],
    batch_per_second: int,) -> None:
    global SHUTDOWN_FLAG

    writer_index = 0
    current_second = int(time.time())
    batches_queued_this_second = 0

    while not SHUTDOWN_FLAG:
        now = int(time.time())

        if now != current_second:
            Logger.log(f"[DISTRIBUTE BATCH] {batches_queued_this_second} Batches has been queued the following second {current_second}.", LogType.DEBUG)
            current_second = now
            batches_queued_this_second = 0

        if batch_per_second != -1 and batches_queued_this_second >= batch_per_second:
            time.sleep(0.005)
            continue

        try:
            batch = next(gen)
        except StopIteration:

            return

        try:
            workers[writer_index].queue.put(batch, timeout=0.002)
        except queue.Full:
            Logger.log(f"[DISTRIBUTE BATCH] Node={workers[writer_index].address} queue is full, drop Batch for second {now}", LogType.WARN)


        writer_index = (writer_index + 1) % len(workers)
        batches_queued_this_second += 1




#### Helper Functions

def create_writer_workers(nodes: list[str], batch_per_second: int, schema: pa.Schema) -> list[WriteWorker]:
    Logger.log("Creating Write Worker", LogType.INFO)
    workers: list[WriteWorker] = []
    for node in nodes:
        queue_size: int = batch_per_second
        w = WriteWorker(node, queue_size, schema)
        t = threading.Thread(target=send_batch, args=(w,), daemon=True)
        w.thread = t
        t.start()
        workers.append(w)
    return workers

def shutdown_workers(workers: list[WriteWorker]) -> None:
    Logger.log("Shutting down Write Worker", LogType.INFO)
    for worker in workers:
        try:
            worker.queue.shutdown(immediate=True)
        except Exception:
            pass
    for worker in workers:
        worker.thread.join(timeout=20)

def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="WoolMilk Source Node")
    parser.add_argument(
        "--advertised-host", type=str, default="localhost", help="URL reachable from Outside"
    )
    parser.add_argument(
        "--port",
        help="Port for Apache Flight Server to listen on",
        type=int,
        default=8000,
    )
    parser.add_argument(
        "--forward-nodes",
        help="Flight server address (host:port,host:port)",
        type=str,
        default="localhost:8010",
    )

    parser.add_argument(
        "--tuple-per-batch",
        type=int,
        help="Number of Tuples to send per Batch",
        default=10 ** 4,
    )

    parser.add_argument(
        "--batch-per-second",
        type=int,
        help="Number of Batches to send per second (per forward node) (Limit, -1 if uncapped)",
        default=80,
    )

    parser.add_argument(
        "--overall_batches",
        type=int,
        help="Number of overall batches to send (Limit, -1 if uncapped)",
        default=-1,
    )

    parser.add_argument(
        "--file-path",
        type=str,
        default="data/test.parquet",
        help="Path to the Parquet file containing Nexmark data",
    )

    parser.add_argument(
        "--log-save-level", type=LogType, default=LogType.WARN, help="Log level"
    )
    parser.add_argument(
        "--log-print-level", type=LogType, default=LogType.DEBUG, help="Log level"
    )
    parser.add_argument(
        "--monitor_url", type=str, default=None, help="Monitor URL"
    )
    args: argparse.Namespace = parser.parse_args()

    print("\n" + "=" * 40)
    print(" WoolMilk Source Node Parameters")
    print("=" * 40)

    print(f" Host                       : {args.advertised_host}")
    print(f" Port                       : {args.port}")
    print(f" Monitor                    : {args.monitor_url}")
    print(f" Processing Nodes           : {args.forward_nodes}")
    print(f" Overall Batches            : {args.overall_batches}")
    print(f" Batch per Second           : {args.batch_per_second}")
    print(f" Tuple per Batch            : {args.tuple_per_batch}")
    print(f" Parquet File               : {args.file_path}")
    print("=" * 40 + "\n")
    return args



if __name__ == "__main__":
    args = parse_arguments()

    Logger = LogService(args.log_save_level, args.log_print_level)

    Logger.log("Starting WoolMilk Source Node", LogType.INFO)

    parquet_file: pq.ParquetFile = pq.ParquetFile(args.file_path)
    schema: pa.Schema = parquet_file.schema_arrow
    schema = schema.append(pa.field("timestamp", pa.int64()))
    gen = parse_parquet_file(parquet_file, args.overall_batches, args.tuple_per_batch)

    nodes: list[str] = [n.strip() for n in args.forward_nodes.split(",") if n.strip()]
    workers: list[WriteWorker] = create_writer_workers(nodes, args.batch_per_second * 2, schema)

    monitorService: MonitorService | None = None
    if args.monitor_url:
        monitorService = MonitorService(Logger, monitor_url=f"grpc://{args.monitor_url}")

    source_node = SourceNode(args.port, args.advertised_host, monitorService)
    server_thread = threading.Thread(target=source_node.serve, args=(), daemon=True)

    try:
        server_thread.start()
        distribute_batches(gen, workers, args.batch_per_second)

    except KeyboardInterrupt:
        SHUTDOWN_FLAG = True

    finally:
        if source_node.monitor and source_node.monitor.monitor_connected:
            source_node.monitor.disconnect_monitor(f"{args.advertised_host}:{args.port}")
        shutdown_workers(workers)
        Logger.log("Shutting down WoolMilk Source Node", LogType.INFO)
        source_node.shutdown()
        server_thread.join(timeout=3)
        Logger.log("Bye!", LogType.INFO)