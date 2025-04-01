import time
import pyarrow as pa
import pyarrow.flight as fl
import json
from pyarrow._flight import Ticket

class ProcessorNode:
    """
    A ProcessorNode connects to multiple entry nodes and a single exit node.
    It reads data from entry nodes using a round-robin strategy and forwards it to the exit node.
    """
    def __init__(self, entry_endpoints, exit_host, exit_port, node_id=""):
        """
        Args:
            entry_endpoints: List of (host, port) tuples for entry nodes.
            exit_host: Host for the exit node.
            exit_port: Port for the exit node.
            node_id: Identifier for this processor node.
        """
        # current node id
        self.node_id = node_id
        self.entry_clients = []
        self.client_states = []  # Each state: { "client": FlightClient, "completed": bool, "ticket": Ticket }

        # init clients states
        for (host, port) in entry_endpoints:
            location = f"grpc://{host}:{port}"
            client = fl.FlightClient(location)
            self.entry_clients.append(client)
            # Initialize state; ticket will be set for each command in run_command.
            self.client_states.append({
                "client": client,
                "completed": False,
                "ticket": None,
                "id": location
            })
            print(f"[{self.node_id}] Connected to entry node at {location}")

        if exit_host is not None and exit_port is not None:
            exit_location = f"grpc://{exit_host}:{exit_port}"
            self.exit_client = fl.FlightClient(exit_location)
            print(f"[{self.node_id}] Connected to exit node at {exit_location}\n")
        
    def initialize_client_states(self, command: str):
        """
        For the given command, reset each client's state: mark as not done
        and create a new ticket based on node_id and command.

        Args:
            command: The command to run.
        """
        for state in self.client_states:
            config = {"node_id": self.node_id, "command": command}
            config_str = json.dumps(config)
            ticket = Ticket(config_str)
            state["ticket"] = ticket
            state["completed"] = False
    
    def read_next_batch(self, state):
        """
        Reads the next batch from a single client using its stored ticket.
        Returns the chunk table (can be empty).

        Args:
            state: The client state to read from.
        """
        client = state["client"]
        ticket = state["ticket"]
        reader = client.do_get(ticket)
        chunk_table = reader.read_all()
        return chunk_table
    
    def pass_to_exit_node(self, command, chunk_table):
        """
        Forwards the given chunk_table to the exit node.
        Args:
            command: The command to run.
            chunk_table: The table to forward.
        """
        
        if self.exit_client is None:
            return

        descriptor = fl.FlightDescriptor.for_command(command)
        # Write the table to the exit node.
        writer, _ = self.exit_client.do_put(descriptor, chunk_table.schema)
        writer.write_table(chunk_table)
        writer.close()
    
    def run_command(self, command: str):
        """
        Uses a round-robin strategy over all entry nodes: repeatedly polls each node
        until all report no more data. Each non-empty chunk is immediately sent to the exit node.

        Args:
            command: The command to run.
        """
        self.initialize_client_states(command)
        total_rows = 0
        data_mb = 0
        start_time = time.time()

        while True:
            completed = True # Assume all clients are done.
            for state in self.client_states:
                if state["completed"]:
                    continue

                completed = False
                chunk_table = self.read_next_batch(state)

                if chunk_table.num_rows == 0:
                    state["completed"] = True
                else:
                    total_rows += chunk_table.num_rows
                    data_mb += chunk_table.nbytes / 1024 / 1024
                    # Forward the chunk to the exit node.
                    self.pass_to_exit_node(command, chunk_table)
                    print(f"[{self.node_id}]: Processed {chunk_table.num_rows} rows from client [{state["id"]}].")

            if completed:
                break

        end_time = time.time()

        print(f"[{self.node_id}] Command '{command}' completed. Total rows processed: {total_rows} ===")
        print(f"[{self.node_id}] Total time: {end_time - start_time:.2f} seconds")
        print(f"[{self.node_id}] Total data: {data_mb:.2f} MB")
        print(f"[{self.node_id}] Data rate: {data_mb / (end_time - start_time):.2f} MB/s\n")
    
    def run_all_commands(self):
        commands = ["nexmarkq1", "nexmarkq2"]
        for command in commands:
            self.run_command(command)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Test the ProcessorNode")
    parser.add_argument("--entry_nodes", type=str, required=True,
                        help='Entry nodes as a JSON string, e.g. \'[["localhost", 8815]]\'')
    parser.add_argument("--exit_host", type=str, default="localhost", help="Exit node host")
    parser.add_argument("--exit_port", type=int, default=8820, help="Exit node port")
    parser.add_argument("--node_id", type=str, default="round_robin_processor", help="Processor node identifier")
    args = parser.parse_args()

    try:
        # Parse entry nodes as a list of (host, port) tuples.
        entry_nodes = json.loads(args.entry_nodes)
    except Exception as e:
        print("Error parsing entry_nodes JSON:", e)
        exit(1)

    print(f"Starting ProcessorNode with entry nodes: {entry_nodes}")
    processor = ProcessorNode(entry_nodes, args.exit_host, args.exit_port, node_id=args.node_id)
    print(f"Running all commands on ProcessorNode {args.node_id}")
    processor.run_all_commands()
    print(f"ProcessorNode {args.node_id} completed")