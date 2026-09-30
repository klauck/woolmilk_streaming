import argparse
import json
import os
import threading
import time
from collections import Counter

import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import flight

from woolmilk.encoding import dictionary_decode_batch
from woolmilk.runtime_config import RuntimeConfig
from woolmilk.source_node import NodeStatus, SourceNodeActions


class SinkNode(flight.FlightServerBase):
    def __init__(self, location, result_folder=None):
        super().__init__(location)
        self.result_folder = result_folder
        if result_folder:
            os.makedirs(result_folder, exist_ok=True)
        self.file_counter = 0
        self.file_counter_lock = threading.Lock()
        self.logs = []
        self.logs_lock = threading.Lock()
        self.open_requests = 0
        self.open_requests_lock = threading.Lock()
        self.totals = Counter()
        self.totals_lock = threading.Lock()
        self.started_at = time.time()
        self.runtime_config = RuntimeConfig()

    def node_status(self):
        with self.open_requests_lock:
            busy = self.open_requests > 0
        return NodeStatus.RECEIVING_DATA if busy else NodeStatus.IDLE

    def metrics(self):
        with self.totals_lock:
            return dict(self.totals, uptime=time.time() - self.started_at)

    def do_action(self, context, action):
        if action.type == "get_logs":
            with self.logs_lock:
                logs = self.logs
            yield flight.Result(json.dumps(logs).encode("utf-8"))
        elif action.type == "delete_logs":
            with self.logs_lock:
                self.logs = []
        elif action.type == "SET_CONFIG":
            try:
                cfg = RuntimeConfig.from_json(bytes(action.body.to_pybytes()))
            except Exception as e:
                yield flight.Result(f"ERR:{e}".encode("utf-8"))
                return
            self.runtime_config = cfg
            print(f"SET_CONFIG applied: {cfg}")
            yield flight.Result(b"OK")
        elif action.type == SourceNodeActions.GET_STATUS:
            yield flight.Result(self.node_status().encode("utf-8"))
        elif action.type == SourceNodeActions.GET_INFO:
            info = {
                "role": "sink",
                "status": self.node_status(),
                "config": {"result_folder": self.result_folder},
                "metrics": self.metrics(),
                "targets": [],
            }
            yield flight.Result(json.dumps(info).encode("utf-8"))
        else:
            raise NotImplementedError(f"Unknown action: {action.type}")

    def do_put(self, context, descriptor, reader, writer):
        with self.open_requests_lock:
            self.open_requests += 1
        try:
            self.process_stream(descriptor, reader)
        finally:
            with self.open_requests_lock:
                self.open_requests -= 1

    def process_stream(self, descriptor, reader):
        experiment_id = None
        iteration_id = None
        source_node_id = None
        thread_id = None

        try:
            incoming_path_info = json.loads(descriptor.path[0].decode("utf-8"))
            if isinstance(incoming_path_info, dict):
                experiment_id = incoming_path_info.get("experiment_id")
                iteration_id = incoming_path_info.get("iteration_id")
                source_node_id = incoming_path_info.get("source_node_id")
                thread_id = incoming_path_info.get("thread_id")
        except (json.JSONDecodeError, UnicodeDecodeError, AttributeError):
            pass

        total_bytes = 0
        receive_times = []
        work_times = []
        batch_start = start = time.time()
        result = []
        for chunk in reader:
            batch_work_start = time.time()
            batch = chunk.data
            batch_id = (
                bytes(chunk.app_metadata).decode("utf-8") if chunk.app_metadata else None
            )

            batch = dictionary_decode_batch(batch)

            batch_bytes = batch.nbytes
            total_bytes += batch_bytes
            if self.result_folder:
                result.append(batch)

            batch_work_end = time.time()
            work_times.append((batch_work_start, batch_work_end, batch_id, batch_bytes))

            receive_times.append((batch_start, batch_work_end, batch_id, batch_bytes))
            batch_start = batch_work_end
            with self.totals_lock:
                self.totals["rows_in"] += batch.num_rows
                self.totals["bytes_in"] += batch_bytes
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
            "receive_times": receive_times,
            "work_times": work_times,
            "end_time": end,
        }
        with self.logs_lock:
            self.logs.append(log)
        log_str = json.dumps(log)
        print(log_str)

        with self.file_counter_lock:
            local_id = self.file_counter
            self.file_counter += 1

        if self.result_folder:
            if result:
                # Result is not empty
                table = pa.Table.from_batches(result)
                pq.write_table(table, f"{self.result_folder}/{local_id}.parquet")
                print(f"Wrote .. {self.result_folder}/{local_id}.parquet")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="WoolMilk Sink Node")
    parser.add_argument(
        "--port", type=int, default=8020, help="Port to run the WoolMilk sink node"
    )
    parser.add_argument(
        "--result-folder", type=str, default="None", help="Folder to store results"
    )
    args = parser.parse_args()

    print("\n" + "=" * 40)
    print(" WoolMilk Sink Node Parameters")
    print("=" * 40)
    print(f" Port        : {args.port}")
    print("=" * 40 + "\n")

    location = f"grpc://0.0.0.0:{args.port}"
    result_folder = args.result_folder
    if args.result_folder == "None":
        result_folder = None
    sink_node = SinkNode(location, result_folder)
    print(f"WoolMilk sink node running at {location}")
    sink_node.serve()
