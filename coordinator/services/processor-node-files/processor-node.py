#!/usr/bin/env python3
import time
import pyarrow as pa
import pyarrow.flight as fl
from pyarrow._flight import Ticket

class ProcessorNode:
    """
    A ProcessorNode connects to multiple entry nodes and a single exit node.
    It reads data from entry nodes in a round-robin style
    and optionally forwards it to the exit node.
    """
    def __init__(self, entry_endpoints, exit_host=None, exit_port=None, node_id=""):
        self.node_id = node_id
        self.entry_endpoints = entry_endpoints
        self.exit_client = None
        if exit_host is not None and exit_port is not None:
            exit_location = f"grpc://{exit_host}:{exit_port}"
            self.exit_client = fl.FlightClient(exit_location)
            print(f"[{self.node_id}] Connected to exit node at {exit_location}\n")

    def initialize_client_states(self, command: str, endpoints):
        """
        For the given command (SQL query), reset each client's state by setting a new ticket
        containing 'command_str'.
        """
        client_states = []
        for ep in endpoints:
            location = f"grpc://{ep['host']}:{ep['port']}"
            client = fl.FlightClient(location)
            config_str = f'{{"node_id":"{self.node_id}","command_str":"{command}"}}'
            ticket = Ticket(config_str.encode("utf-8"))
            client_states.append({
                "client": client,
                "completed": False,
                "ticket": ticket,
                "id": location
            })
        return client_states

    def read_next_batch(self, state):
        reader = state["client"].do_get(state["ticket"])
        return reader.read_all()

    def pass_to_exit_node(self, command, chunk_table):
        if self.exit_client is None:
            return
        descriptor = fl.FlightDescriptor.for_command(command)
        writer, _ = self.exit_client.do_put(descriptor, chunk_table.schema)
        writer.write_table(chunk_table)
        writer.close()

    def run_command(self, command: str, endpoints):
        client_states = self.initialize_client_states(command, endpoints)
        total_rows = 0
        data_mb = 0
        start_time = time.time()

        while True:
            completed = True
            for state in client_states:
                if state["completed"]:
                    continue
                completed = False
                chunk_table = self.read_next_batch(state)
                if chunk_table.num_rows == 0:
                    state["completed"] = True
                else:
                    num_rows = chunk_table.num_rows
                    total_rows += num_rows
                    print(f"[{self.node_id}] Received {num_rows} rows from {state['id']}")
            if completed:
                break

        end_time = time.time()
        duration = end_time - start_time

        print(f"\n[{self.node_id}] Command '{command}' completed.")
        print(f"  Total rows processed: {total_rows}")
        print(f"  Total time: {duration:.2f} seconds")

    def run_all_commands(self, queries):
        for item in queries:
            node_id = item["node_id"]
            commands = item["queries_string"]
            matching_endpoints = [ep for ep in self.entry_endpoints if node_id in ep.get("services", [])]
            if not matching_endpoints:
                print(f"[{self.node_id}] No endpoints match services for node_id '{node_id}'. Skipping.\n")
                continue
            for command in commands:
                self.run_command(command, matching_endpoints)

def parse_entry_endpoints(endpoints_str):
    result = []
    blocks = endpoints_str.split(";")
    for block in blocks:
        block = block.strip()
        if not block:
            continue
        parts = block.split("|")
        name = parts[0].strip()
        host = parts[1].strip()
        port = int(parts[2].strip())
        if len(parts) > 3 and parts[3].strip():
            services = [s.strip() for s in parts[3].split(",") if s.strip()]
        else:
            services = []
        result.append({
            "name": name,
            "host": host,
            "port": port,
            "services": services
        })
    return result

def parse_queries(queries_str):
    result = []
    blocks = queries_str.split(";")
    for block in blocks:
        block = block.strip()
        if not block:
            continue
        parts = block.split("|")
        node_id = parts[0].strip()
        if len(parts) > 1 and parts[1].strip():
            queries_list = [q.strip() for q in parts[1].split(",") if q.strip()]
        else:
            queries_list = []
        result.append({
            "node_id": node_id,
            "queries_string": queries_list
        })
    return result

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="ProcessorNode")
    parser.add_argument(
        "--entry_endpoints",
        type=str,
        default="entry1|localhost|8815|bids;",
    )
    parser.add_argument(
        "--queries",
        type=str,
        default="bids|SELECT * FROM bids;",
    )
    parser.add_argument("--exit_host", type=str, default=None)
    parser.add_argument("--exit_port", type=int, default=None)
    parser.add_argument("--node_id", type=str, default="json_processor")
    args = parser.parse_args()

    entry_endpoints = parse_entry_endpoints(args.entry_endpoints)
    queries = parse_queries(args.queries)

    print(f"[{args.node_id}] Starting ProcessorNode\n")
    processor = ProcessorNode(
        entry_endpoints=entry_endpoints,
        exit_host=args.exit_host,
        exit_port=args.exit_port,
        node_id=args.node_id
    )
    processor.run_all_commands(queries)
    print(f"[{args.node_id}] Completed all queries.\n")
