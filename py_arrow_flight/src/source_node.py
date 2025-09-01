import argparse
import sys
import threading
import time

import pyarrow as pa
import pyarrow.flight
from data_generator import NexmarkDataGenerator


class SourceNode:
    """
    WoolMilk source node for sending Nexmark data streams.
    This class connects to the Arrow Flight server and sends data streams
    for different Nexmark events (bid, auction, person, etc.).
    It uses a stream provider to generate the data and sends it in batches.
    """

    def __init__(self, stream_provider, servers, thread_id=0):
        self.server_addresses = servers
        self.stream_provider = stream_provider
        self.thread_id = thread_id

    def send_data_to_node(self, client, tbl_name, tbl, address):
        if tbl is not None and tbl.num_rows > 0:
            writer, _ = client.do_put(
                pa.flight.FlightDescriptor.for_path(f"{tbl_name}-stream"), tbl.schema
            )

            start = time.time()
            for batch in tbl.to_batches(max_chunksize=65536):
                writer.write_batch(batch)
            writer.done_writing()
            end = time.time()

            total_bytes = tbl.nbytes
            duration = end - start
            mbps = (total_bytes * 8) / (duration * 1024 * 1024)
            print(
                f'WM_LOG= {{"THREAD": "{self.thread_id}:{tbl_name}@{address}", "send_bytes": {total_bytes}, "start_time": {start}, "duration": {duration}, "Mbps": {mbps:.2f}}}'
            )

    def start(self):
        """Start the client to send data streams."""
        print(
            f"Starting client for thread {self.thread_id} with server(s) {self.server_addresses}"
        )
        stream = self.stream_provider.get_stream()
        server_count = len(self.server_addresses)
        set_idx = 0
        # stream yields (person_tbl, auction_tbl, bid_tbl, category_tbl)
        for person_tbl, auction_tbl, bid_tbl, category_tbl in stream:
            # Pick server in round-robin fashion
            server = self.server_addresses[set_idx % server_count]
            set_idx += 1
            address = f"{server[0]}:{server[1]}"
            client = pa.flight.FlightClient(f"grpc://{address}")

            self.send_data_to_node(client, "person", person_tbl, address)
            self.send_data_to_node(client, "auction", auction_tbl, address)
            self.send_data_to_node(client, "bid", bid_tbl, address)
            self.send_data_to_node(client, "category", category_tbl, address)


class StreamProvider:
    """
    Base class for stream providers.
    This class is responsible for providing the stream data based on the event type.
    """

    def __init__(self):
        pass

    @staticmethod
    def getStreamProvider(event: str, tuples_per_batch, overall_tuples, executable):
        """Get the stream provider based on the event type."""
        try:
            stream_provider, stream_type = event.split(".")
        except ValueError:
            raise ValueError("Invalid event format. Expected 'nexmark.<stream_type>'")

        if stream_provider != "nexmark":
            raise ValueError("Invalid stream provider. Expected 'nexmark'")

        return NexmarkStreamProvider(
            stream_type, tuples_per_batch, overall_tuples, executable
        )


class NexmarkStreamProvider(StreamProvider):
    """
    Stream provider for Nexmark data.
    This class generates data for the specified Nexmark event type.
    """

    def __init__(self, stream_type, tuples_per_batch, overall_tuples, executable):
        super().__init__()
        self.stream_type = stream_type
        self.tuples_per_batch = tuples_per_batch
        self.overall_tuples = overall_tuples
        self.data_generator = NexmarkDataGenerator(
            event_type=stream_type,
            chunk_size=tuples_per_batch,
            no_records=overall_tuples,
            executable=executable,
        )

    def get_stream(self):
        """Get the stream data."""
        return self.data_generator.generate()


def send_data(
    thread_id, stream, tuples_per_batch, overall_tuples, servers, generator_executable
):
    """Function to send data in a separate thread."""
    stream_provider = StreamProvider.getStreamProvider(
        stream, tuples_per_batch, overall_tuples, generator_executable
    )
    source_node = SourceNode(stream_provider, servers, thread_id)
    source_node.start()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="WoolMilk Source Node")
    parser.add_argument(
        "--stream",
        choices=["nexmark.bid", "nexmark.auction", "nexmark.person"],
        default="nexmark.bid",
        help="Stream type",
    )
    parser.add_argument(
        "--generator-executable", default="nexmark", help="Executable to generate data"
    )
    parser.add_argument(
        "--overall-tuples",
        type=int,
        default=1000000,
        help="Total number of records needs to be sent.",
    )
    parser.add_argument(
        "--tuples-per-batch", type=int, default=10000, help="Tuple rate (number)"
    )
    parser.add_argument(
        "--processing-servers",
        help="Flight server address (host:port,host:port)",
        type=str,
        default="localhost:8815",
    )
    parser.add_argument(
        "--thread-count",  # thread count is used to send data to same server but multiple threads, this is for testing purpose.
        type=int,
        help="Number of threads to use for sending data",
        default=1,
    )
    args = parser.parse_args()

    print("\n" + "=" * 40)
    print(" WoolMilk Source Node Parameters")
    print("=" * 40)
    print(f" Stream Type                : {args.stream}")
    print(f" Overall Tuples             : {args.overall_tuples}")
    print(f" Tuples Per Batch           : {args.tuples_per_batch}")
    print(f" Processing Servers         : {args.processing_servers}")
    print(f" Thread Count               : {args.thread_count}")
    print(f" Generator Executable       : {args.generator_executable}")
    print("=" * 40 + "\n")

    servers = []
    for address in args.processing_servers.split(","):
        host, port = address.split(":")
        servers.append((host, int(port)))

    if len(servers) == 0:
        print("No server addresses provided. Exiting.")
        sys.exit(1)

    threads = []
    for thread_id in range(args.thread_count):
        t = threading.Thread(
            target=send_data,
            args=(
                thread_id,
                args.stream,
                args.tuples_per_batch,
                args.overall_tuples,
                servers,
                args.generator_executable,
            ),
        )
        threads.append(t)
        t.start()

    for t in threads:
        t.join()
