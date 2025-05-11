#!/usr/bin/env python3
import time
from typing import List
import pyarrow.flight as fl
from pyarrow._flight import Ticket
from api_models import EntryEndpoint, Query
import pyarrow as pa, pyarrow.ipc as pa_ipc

class ProcessorNode:
    """
    A ProcessorNode connects to multiple entry nodes and a single exit node.
    It reads data from entry nodes in a round-robin style
    and optionally forwards it to the exit node.
    """
    def __init__(self, node_id: str, entry_endpoints: List[EntryEndpoint], exit_host: str | None = None, exit_port: int | None=None):
        self.node_id = node_id
        self.entry_endpoints = entry_endpoints
        self.exit_client = None
        if exit_host is not None and exit_port is not None:
            exit_location = f"grpc://{exit_host}:{exit_port}"
            self.exit_client = fl.FlightClient(exit_location)
            print(f"[{self.node_id}] Connected to exit node at {exit_location}\n")

    def initialize_client_states(self, command: str, endpoints: List[EntryEndpoint]):
        """
        For the given command (SQL query), reset each client's state by setting a new ticket
        containing 'command_str'.
        """
        client_states = []
        for ep in endpoints:
            location = f"grpc://{ep.host}:{str(ep.port)}"
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

    def table_size_bytes(self, tbl: pa.Table) -> int:
        try:
            return tbl.nbytes                         
        except pa.ArrowTypeError:
            pass                                      
        except AttributeError:
            pass

        sink = pa.BufferOutputStream()
        
        with pa_ipc.new_stream(sink, tbl.schema) as writer:
            writer.write_table(tbl)
        return sink.getvalue().size


    def run_command(self, command: str, endpoint):
        client_states = self.initialize_client_states(command, endpoint)

        print(f"\n[{self.node_id}] Running command '{command}' on {client_states} entry nodes.")

        stats = {}

        while True:
            completed = True
            for state in client_states:
                if state["completed"]:
                    continue
                
                start_time = time.time()
                current_stats = stats.get(state["id"], {
                    "rows": 0,
                    "data_mb": 0,
                    "total_time": 0,
                })

                completed = False
                chunk_table = self.read_next_batch(state)
                if chunk_table.num_rows == 0:
                    state["completed"] = True
                else:
                    current_stats["rows"] += chunk_table.num_rows
                    current_stats["data_mb"] += self.table_size_bytes(chunk_table) / (1024 * 1024)
                    print(f"[{self.node_id}] Received {chunk_table.num_rows} rows from {state['id']}")

                current_stats["total_time"] = current_stats["total_time"] + (time.time() - start_time)
                stats[state["id"]] = current_stats

            if completed:
                break

        print(f"\n[{self.node_id}] Command '{command}' completed.")

        for (key, value) in stats.items():
            value["mbps"] = value["data_mb"] / value["total_time"]
            print(f"==== ENTRY NODE: [{key}] ====")
            print(f"Rows: {value['rows']}")
            print(f"Data MB: {value['data_mb']:.2f}")
            print(f"Time: {value['total_time']:.2f} seconds")
            # print rate in mbps
            print(f"Rate: {value['mbps']:.2f} MB/s")

        return stats

    def run_query(self, query: Query):
        qn = query.name
        query_str = query.query
        
        matching_endpoints = []

        for ep in self.entry_endpoints:
            if qn in ep.query_names:
                matching_endpoints.append(ep)

        if len(matching_endpoints) == 0:
            print(f"[{self.node_id}] No matching endpoint for query '{qn}'")
            return
        
        return self.run_command(query_str, matching_endpoints)