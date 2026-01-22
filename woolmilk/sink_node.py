import argparse
import json
import os
import threading
import time

import pyarrow as pa
import pyarrow.flight
import pyarrow.parquet as pq


class SinkNode(pa.flight.FlightServerBase):
    def __init__(self, location, result_folder=None):
        super().__init__(location)
        self.result_folder = result_folder
        if result_folder:
            os.makedirs(result_folder, exist_ok=True)
        self.file_counter = 0
        self.file_counter_lock = threading.Lock()

    def do_put(self, context, descriptor, reader, writer):
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
        start = receive_start = time.time()
        result = []
        for chunk in reader:
            batch = chunk.data
            # execute and forward data here
            total_bytes += batch.nbytes
            if self.result_folder:
                result.append(batch)
            receive_end = time.time()
            receive_times.append((receive_start, receive_end))
            receive_start = receive_end
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
            "end_time": end,
        }
        log_str = json.dumps(log)

        print(f"WM_LOG= {log_str}")
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
