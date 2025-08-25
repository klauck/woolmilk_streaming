import pyarrow as pa
import pyarrow.flight
import sys
import time
import argparse

class SinkNode(pa.flight.FlightServerBase):
    def __init__(self, location):
        super().__init__(location)

    def do_put(self, context, descriptor, reader, writer):
        total_bytes = 0
        start = time.time()
        for chunk in reader:
            batch = chunk.data
            # execute and forward data here
            total_bytes += batch.nbytes
        end = time.time()

        duration = end - start
        mbps = (total_bytes * 8) / (duration * 1024 * 1024)
        print(f"Received {total_bytes} bytes in {duration:.2f} seconds ({mbps:.2f} Mbps)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="WoolMilk Sink Node")
    parser.add_argument(
        "--server-address",
        type=str,
        default="0.0.0.0:8820",
        help="Address to run the WoolMilk sink node (host:port)")
    args = parser.parse_args()

    print("\n" + "="*40)
    print(" WoolMilk Sink Node Parameters")
    print("="*40)
    print(f" Address        : {args.server_address}")
    print("="*40 + "\n")

    location = f"grpc://{args.server_address}"
    sink_node = SinkNode(location)
    print(f"WoolMilk sink node running at {location}")
    sink_node.serve()
