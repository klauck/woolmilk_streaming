import pyarrow as pa
import pyarrow.flight
import time
import sys
import threading
import argparse
from sources import StreamProvider

class SourceNode:
    """
    WoolMilk source node for sending Nexmark data streams.
    This class connects to the Arrow Flight server and sends data streams
    for different Nexmark events (bid, auction, person, etc.).
    It uses a stream provider to generate the data and sends it in batches.
    """
    def __init__(self, stream_provider, tuples_per_second, servers, thread_id=0):
        self.server_addresses = servers
        self.stream_provider = stream_provider
        self.thread_id = thread_id
        self.tuples_per_second = tuples_per_second

    def print(self, message):
        print(f"THREAD:[{self.thread_id}]:{message}")

    def send_data_to_node(self, client, tbl_name, tbl, address):
        if tbl is not None and tbl.num_rows > 0:
            writer, _ = client.do_put(
                pa.flight.FlightDescriptor.for_path(f"{tbl_name}-stream"),
                tbl.schema
            )

            start_time = time.time()
            total_tuples_sent = 0
            total_bytes = 0

            time_per_tuple = 1.0 / self.tuples_per_second
            self.print(f"{tbl_name}@{address}: Target: {self.tuples_per_second} tuples/sec, time per tuple: {time_per_tuple:.6f}s")

            for batch in tbl.to_batches(max_chunksize=65536):
                batch_start = time.time()
                writer.write_batch(batch)
                batch_end = time.time()
                
                total_tuples_sent += batch.num_rows
                total_bytes += batch.nbytes

                required_time = batch.num_rows * time_per_tuple
                actual_send_time = batch_end - batch_start
                
                if required_time > actual_send_time:
                    sleep_time = required_time - actual_send_time
                    self.print(f"{tbl_name}@{address}: Sent {batch.num_rows} tuples in {actual_send_time:.4f}s, sleeping {sleep_time:.4f}s")
                    time.sleep(sleep_time)
                else:
                    self.print(f"{tbl_name}@{address}: Sent {batch.num_rows} tuples in {actual_send_time:.4f}s, no sleep needed")

            writer.done_writing()
            end_time = time.time()
            
            total_duration = end_time - start_time
            actual_rate = total_tuples_sent / total_duration if total_duration > 0 else 0
            mbps = (total_bytes * 8) / (total_duration * 1024 * 1024) if total_duration > 0 else 0
            
            self.print(f"{tbl_name}@{address}: ====> {actual_rate:.0f} tuples/sec, {total_tuples_sent} tuples in {total_duration:.2f}s, {mbps:.2f} Mbps (Target: {self.tuples_per_second})")
            
    
    def start(self):
        """Start the client to send data streams."""
        print(f"Starting client for thread {self.thread_id} with server(s) {self.server_addresses}")
        stream = self.stream_provider.get_stream()
        server_count = len(self.server_addresses)
        set_idx = 0
        
        for tables_dict in stream:
            # Pick server in round-robin fashion
            server = self.server_addresses[set_idx % server_count]
            set_idx += 1
            address = f"{server[0]}:{server[1]}"
            client = pa.flight.FlightClient(f"grpc://{address}")
            
            # Send only the tables that exist in the dictionary
            for table_name, table in tables_dict.items():
                if table is not None:
                    self.send_data_to_node(client, table_name, table, address)

def send_data(thread_id, stream, tuples_per_batch, overall_tuples, tuples_per_second, servers, generator_executable):
    """Function to send data in a separate thread."""
    stream_provider = StreamProvider.get_stream_provider(stream, tuples_per_batch, overall_tuples, generator_executable)
    source_node = SourceNode(stream_provider, tuples_per_second, servers, thread_id)
    source_node.start()
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="WoolMilk Source Node")
    parser.add_argument(
        "--stream",
        choices=["nexmark.bid", "nexmark.auction", "nexmark.person", "nexmark.category",
                 "custom.random"],
        default="nexmark.bid",
        help="Stream type (provider.stream_type). Providers: nexmark, custom"
    )
    parser.add_argument(
        "--generator-executable",
        default="nexmark",
        help="Executable to generate data"
    )
    parser.add_argument(
        "--tuples-per-batch",
        type=int,
        default=10000,
        help="Tuple rate (number)"
    )
    parser.add_argument(
        "--overall-tuples",
        type=int,
        default=1000000,
        help="Total number of records needs to be sent."
    )
    parser.add_argument(
        "--processing-servers",
        help="Flight server address (host:port,host:port)",
        type=str,
        default="localhost:8815"
    )
    parser.add_argument(
        "--thread-count",#thread count is used to send data to same server but multiple threads, this is for testing purpose.
        type=int,
        help="Number of threads to use for sending data",
        default=1,
    )
    parser.add_argument(
        "--tuples-per-second",
        type=int,
        help="Number of tuples to send per second",
        default=10000
    )
    args = parser.parse_args()

    print("\n" + "="*40)
    print(" WoolMilk Source Node Parameters")
    print("="*40)
    print(f" Stream Type                : {args.stream}")
    print(f" Tuples Per Batch           : {args.tuples_per_batch}")
    print(f" Overall Tuples             : {args.overall_tuples}")
    print(f" Processing Servers         : {args.processing_servers}")
    print(f" Thread Count               : {args.thread_count}")
    print(f" Generator Executable       : {args.generator_executable}")
    print(f" Tuples Per Second          : {args.tuples_per_second}")
    print("="*40 + "\n")

    servers = []
    for address in args.processing_servers.split(","):
        host, port = address.split(":")
        servers.append((host, int(port)))

    if len(servers) == 0:
        print("No server addresses provided. Exiting.")
        sys.exit(1)

    threads = []
    for thread_id in range(args.thread_count):
        t = threading.Thread(target=send_data, 
                             args=(thread_id, args.stream, 
                                    args.tuples_per_batch, 
                                    args.overall_tuples, 
                                    args.tuples_per_second,
                                    servers,
                                    args.generator_executable)
        )
        threads.append(t)
        t.start()

    for t in threads:
        t.join()
