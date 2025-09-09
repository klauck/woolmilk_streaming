import argparse
import os
import time

import pyarrow as pa
import pyarrow.flight
import pyarrow.parquet as pq


class SinkNode(pa.flight.FlightServerBase):
    def __init__(self, location, result_folder=None):
        super().__init__(location)
        os.makedirs(result_folder, exist_ok=True)
        self.result_folder = result_folder
        self.file_counter = 0

    def do_put(self, context, descriptor, reader, writer):
        total_bytes = 0
        receive_times = []
        start = receive_start = time.time()
        result = []
        for chunk in reader:
            batch = chunk.data
            # execute and forward data here
            total_bytes += batch.nbytes
            if result_folder:
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
        self.file_counter += 1
        if result_folder:
            table = pa.Table.from_batches(result)
            pq.write_table(table, f"{result_folder}/{self.file_counter}.parquet")


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
