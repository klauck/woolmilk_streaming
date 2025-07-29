import pyarrow as pa
import pyarrow.flight
import time
import sys
import threading
import argparse
from data_generator import NexmarkDataGenerator

class FlightClient():
    """
    Arrow Flight Client for sending Nexmark data streams.
    This class connects to the Arrow Flight server and sends data streams
    for different Nexmark events (bid, auction, person, etc.).
    It uses a stream provider to generate the data and sends it in batches.
    """
    def __init__(self, stream_providor, servers, thread_id=0):
        self.server_addresses = servers
        self.stream_provider = stream_providor
        self.thread_id = thread_id
    
    def start(self):
        """Start the client to send data streams."""
        print(f"Starting client for thread {self.thread_id} with server(s) {self.server_addresses}")
        # For each server, send the stream data
        # TODO: instead of looping over servers, loop over streams and send data.
        for server in self.server_addresses:
            client = pa.flight.FlightClient(f"grpc://{server[0]}:{server[1]}")
            stream = self.stream_provider.get_stream()
            # stream yields (person_tbl, auction_tbl, bid_tbl, category_tbl)
            for person_tbl, auction_tbl, bid_tbl, category_tbl in stream:
                for tbl, name in [(person_tbl, "person"), (auction_tbl, "auction"), (bid_tbl, "bid"), (category_tbl, "category"),]:
                    if tbl is not None and tbl.num_rows > 0:
                        writer, _ = client.do_put(
                            pa.flight.FlightDescriptor.for_path(f"{name}-stream"),
                            tbl.schema
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
                            f"{self.thread_id}:{name}: Sent {total_bytes} bytes in {duration:.2f} seconds ({mbps:.2f} Mbps)"
                        )

    
class StreamProvider():
    """
    Base class for stream providers.
    This class is responsible for providing the stream data based on the event type.
    """
    def __init__(self):
        pass

    @staticmethod
    def GetStreamProvidor(event: str, tuple_rate, records_count):
        """Get the stream provider based on the event type."""
        splited_event = event.split(".")
        if len(splited_event) != 2:
            raise ValueError("Invalid event format. Expected 'nexmark.<stream_type>'")
        stream_providor = splited_event[0]
        stream_type = splited_event[1]

        if stream_providor != "nexmark":
            raise ValueError("Invalid stream provider. Expected 'nexmark'")
        
        return NexmarkStreamProvider(stream_type, tuple_rate, records_count)
        

class NexmarkStreamProvider(StreamProvider):
    """
    Stream provider for Nexmark data.
    This class generates data for the specified Nexmark event type.
    """
    def __init__(self, stream_type, tuple_rate, records_count):
        super().__init__()
        self.stream_type = stream_type
        self.tuple_rate = tuple_rate
        self.records_count = records_count
        self.data_generator = NexmarkDataGenerator(
            event_type=stream_type,
            chunk_size=tuple_rate,
            no_records=records_count
        )
    
    def get_stream(self):
        """Get the stream data."""
        return self.data_generator.generate()
    
def parse_server_addresses(server_addresses):
    """Parse the server addresses from a string."""
    servers = []
    for address in server_addresses.split(","):
        host, port = address.split(":")
        servers.append((host, int(port)))
    return servers

def send_data(thread_id, stream, tupple_rate, records_count, servers):
    """Function to send data in a separate thread."""
    stream_provider = StreamProvider.GetStreamProvidor(stream, tupple_rate, records_count)
    client = FlightClient(stream_provider, servers, thread_id)
    client.start()
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Arrow Flight Nexmark Client")
    parser.add_argument(
        "--stream",
        choices=["nexmark.bid", "nexmark.auction", "nexmark.person"],
        default="nexmark.bid",
        help="Stream type"
    )
    parser.add_argument(
        "--tuple-rate",
        type=int,
        default=10000,
        help="Tuple rate (number)"
    )
    parser.add_argument(
        "--records-count",
        type=int,
        default=1000000,
        help="Total number of records needs to be sent."
    )
    parser.add_argument(
        "--servers",
        help="Flight server address (host:port,host:port)",
        type=str,
        default="localhost:8815"
    )
    parser.add_argument(
        "--thread-count",
        type=int,
        help="Number of threads to use for sending data",
        default=1,
    )
    args = parser.parse_args()

    print("\n" + "="*40)
    print(" Arrow Flight Nexmark Client Parameters")
    print("="*40)
    print(f" Stream Type    : {args.stream}")
    print(f" Tuple Rate     : {args.tuple_rate}")
    print(f" Records Count  : {args.records_count}")
    print(f" Servers        : {args.servers}")
    print(f" Thread Count   : {args.thread_count}")
    print("="*40 + "\n")

    server_addresses = parse_server_addresses(args.servers)
    if len(server_addresses) == 0:
        print("No server addresses provided. Exiting.")
        sys.exit(1)

    threads = []
    for thread_id in range(args.thread_count):
        t = threading.Thread(target=send_data, 
                             args=(thread_id, args.stream, 
                                    args.tuple_rate, 
                                    args.records_count, 
                                    server_addresses))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()
