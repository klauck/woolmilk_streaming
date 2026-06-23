import argparse
import json
import os
import threading
import time

import pyarrow as pa
import pyarrow.flight
import pyarrow.parquet as pq

from woolmilk.encoding import dictionary_decode_batch
from woolmilk.runtime_config import RuntimeConfig
from woolmilk.source_node import NodeStatus, SourceNodeActions


class SinkNode(pa.flight.FlightServerBase):
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
        self.runtime_config = RuntimeConfig()
        self.runtime_config_lock = threading.Lock()

    def do_action(self, context, action):
        if action.type == "get_logs":
            with self.logs_lock:
                logs = self.logs
            yield pyarrow.flight.Result(json.dumps(logs).encode("utf-8"))
        elif action.type == "delete_logs":
            with self.logs_lock:
                self.logs = []
        elif action.type == "SET_CONFIG":
            try:
                cfg = RuntimeConfig.from_json(bytes(action.body.to_pybytes()))
            except Exception as e:
                yield pyarrow.flight.Result(f"ERR:{e}".encode("utf-8"))
                return
            with self.runtime_config_lock:
                self.runtime_config = cfg
            print(f"SET_CONFIG applied: {cfg}")
            yield pyarrow.flight.Result(b"OK")
        elif action.type == SourceNodeActions.GET_STATUS:
            with self.open_requests_lock:
                if self.open_requests == 0:
                    status = NodeStatus.IDLE
                else:
                    status = NodeStatus.RECEIVING_DATA
            yield pyarrow.flight.Result(status.encode("utf-8"))
        else:
            raise NotImplementedError(f"Unknown action: {action.type}")

    def do_put(self, context, descriptor, reader, writer):
        with self.open_requests_lock:
            self.open_requests += 1

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

        def decode_metadata(meta):
            if meta is None:
                return None
            try:
                return bytes(meta).decode("utf-8")
            except Exception:
                return None

        total_bytes = 0
        receive_times = []
        work_times = []
        batch_start = start = time.time()
        result = []
        for chunk in reader:
            batch_work_start = time.time()
            batch = chunk.data
            batch_id = decode_metadata(chunk.app_metadata)

            batch = dictionary_decode_batch(batch)

            batch_bytes = batch.nbytes
            total_bytes += batch_bytes
            if self.result_folder:
                result.append(batch)

            batch_work_end = time.time()
            work_times.append((batch_work_start, batch_work_end, batch_id, batch_bytes))

            receive_times.append((batch_start, batch_work_end, batch_id, batch_bytes))
            batch_start = batch_work_end
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

        with self.open_requests_lock:
            self.open_requests -= 1


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
