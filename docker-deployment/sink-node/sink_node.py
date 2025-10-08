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

        print(
            f'WM_LOG= {{"received_bytes": {total_bytes}, "start_time": {start},'
            f' "duration": {duration}, "MBps": {mbps:.2f}, "Gbps": {gbps:.4f}}}'
        )
        print(f"End: {end}")
        print("receive_times = ", receive_times)
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
    port = int(os.getenv("PORT", "8020"))
    result_folder = os.getenv("RESULT_FOLDER", "None")

    print("\n" + "=" * 40)
    print(" WoolMilk Sink Node Parameters")
    print("=" * 40)
    print(f" Port        : {port}")
    print("=" * 40 + "\n")

    location = f"grpc://0.0.0.0:{port}"
    if result_folder == "None":
        result_folder = None
    
    print(f"Starting WoolMilk sink node...")
    sink_node = SinkNode(location, result_folder)
    print(f"WoolMilk sink node running at {location}")
    print("Sink node is ready to receive data...")
    sink_node.serve()