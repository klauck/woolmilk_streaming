import argparse
import time

import pyarrow as pa
import pyarrow.flight


class SinkNode(pa.flight.FlightServerBase):
    def __init__(self, location):
        super().__init__(location)

    def do_put(self, context, descriptor, reader, writer):
        total_bytes = 0
        receive_times = []
        start = receive_start = time.time()
        for chunk in reader:
            batch = chunk.data
            # execute and forward data here
            total_bytes += batch.nbytes
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="WoolMilk Sink Node")
    parser.add_argument(
        "--port", type=int, default="8020", help="Port to run the WoolMilk sink node"
    )
    args = parser.parse_args()

    print("\n" + "=" * 40)
    print(" WoolMilk Sink Node Parameters")
    print("=" * 40)
    print(f" Port        : {args.port}")
    print("=" * 40 + "\n")

    location = f"grpc://0.0.0.0:{args.port}"
    sink_node = SinkNode(location)
    print(f"WoolMilk sink node running at {location}")
    sink_node.serve()
