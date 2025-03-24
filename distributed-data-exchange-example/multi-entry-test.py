import os
import time
from multiprocessing import Process

from server_node.GlobalNodeFlightServer import GlobalNodeFlightServer
from processor_node.ProcessorNode import ProcessorNode 
from final_node.ExitNodeFlightServer import ExitNodeFlightServer

def run_entry_node(node_index, port, data_dir):
    """
    Starts an entry node (GlobalNodeFlightServer) that serves data from parquet files.
    Args:
        node_index: Identifier for this entry node.
        port: Port for the entry node.
        data_dir: Directory containing parquet files.
    """
    parquet_files = {
        "bids":     os.path.join(data_dir, f"bid_{node_index}.parquet"),
        "auctions": os.path.join(data_dir, f"auction_{node_index}.parquet"),
        "persons":  os.path.join(data_dir, f"person_{node_index}.parquet"),
    }
    # Global Node Server is Entry Node Server where the data is served from parquet files in shared manner
    server = GlobalNodeFlightServer(
        parquet_registrations=parquet_files,
        host="0.0.0.0",
        port=port
    )
    print(f"[Entry Node {node_index}] Serving on 0.0.0.0:{port}")
    server.serve()

def run_processor_node(entry_endpoints, exit_port, node_id):
    """
    Starts a processor node (ProcessorNode) that connects to all entry nodes and the exit node.
    
    Args:
        entry_endpoints: List of (host, port) tuples for entry nodes.
        exit_port: Port for the exit node.
        node_id: Identifier for this processor node.
    """
    processor = ProcessorNode(
        entry_endpoints=entry_endpoints,
        exit_host="localhost",
        exit_port=exit_port,
        node_id=node_id
    )
    print(f"[Processor Node {node_id}] Connecting to entry nodes: {entry_endpoints} and exit node on port {exit_port}")
    processor.run_all_commands()

def run_exit_node(exit_port):
    """
    Starts the exit node (ExitNodeFlightServer) that receives data from processor nodes.
    
    Args:
        exit_port: Port for the exit node.
    """
    server = ExitNodeFlightServer(host="0.0.0.0", port=exit_port)
    print(f"[Exit Node] Serving on 0.0.0.0:{exit_port}")
    server.serve()

def main():
    """
    Main function that starts all nodes and connects them together. 
    All processors will connect to all entry nodes and a single exit node.
    """
    num_entry_nodes = 3 # Number of entry nodes
    num_processors = 10 # Number of processor nodes
    base_entry_port = 8815 # Base port for entry nodes, each node will use a different port
    exit_port = 8820 # Port for the exit node

    current_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(current_dir, "..", "data")

    # Build a list of entry endpoints
    entry_endpoints = []
    for i in range(1, num_entry_nodes + 1):
        entry_port = base_entry_port + (i - 1)
        entry_endpoints.append(("localhost", entry_port))

    processes = []

    # Start the exit node process first.
    p_exit = Process(target=run_exit_node, args=(exit_port,))
    p_exit.start()
    processes.append(p_exit)
    time.sleep(1)  # Allow the exit node to start

    # Start each entry node as a separate process.
    for i in range(1, num_entry_nodes + 1):
        entry_port = base_entry_port + (i - 1)
        p_entry = Process(target=run_entry_node, args=(i, entry_port, data_dir))
        p_entry.start()
        processes.append(p_entry)
        time.sleep(0.5)  # Slight delay to entry node startup

    # Start all processor nodes concurrently
    processor_processes = []
    for i in range(num_processors):
        node_id = f"processor_{i+1}"
        p_processor = Process(target=run_processor_node, args=(entry_endpoints, exit_port, node_id))
        p_processor.start()
        processor_processes.append(p_processor)
        processes.append(p_processor)
    
    # Wait for all processor processes to finish
    for p in processor_processes:
        p.join()

    # wait for the entry and exit processes, this will block until they finish
    for proc in processes:
        proc.join()

if __name__ == "__main__":
    main()
