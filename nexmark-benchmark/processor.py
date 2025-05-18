#!/usr/bin/env python3
import time
from typing import List
import pyarrow.flight as fl
from pyarrow._flight import Ticket
import pyarrow as pa, pyarrow.ipc as pa_ipc

class ProcessorNode:
    """
    A ProcessorNode connects to multiple entry nodes and a single exit node.
    It reads data from entry nodes in a round-robin style
    and optionally forwards it to the exit node.
    """
    def __init__(self, node_id: str, entry_endpoint):
        self.node_id = node_id
        self.entry_endpoint = entry_endpoint

    def initialize_client_state(self, command: str):
        """
        For the given command (SQL query), reset each client's state by setting a new ticket
        containing 'command_str'.
        """
        location = f"grpc://{self.entry_endpoint['host']}:{str(self.entry_endpoint['port'])}"
        client = fl.FlightClient(location=location, generic_options=[("grpc.max_metadata_size", 64 * 1024)])
        config_str = f'{{"node_id":"{self.node_id}","command_str":"{command}"}}'
        ticket = Ticket(config_str.encode("utf-8"))
        self.client = {
            "client": client,
            "completed": False,
            "ticket": ticket,
            "id": location
        }

        return self.client

    def read_next_batch(self, state):
        reader = state["client"].do_get(state["ticket"])
        return reader.read_all()

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

    def run_command(self, command: str):
        client = self.initialize_client_state(command)

        # print(f"\n[{self.node_id}] Running command '{command}' on entry nodes.")

        stats = {
            "rows": 0,
            "data_mb": 0,
            "total_time": 0,
        }

        while True:
            start_time = time.time()

            chunk_table = self.read_next_batch(client)
            if chunk_table.num_rows == 0:
                client["completed"] = True
                break
            else:
                stats["rows"] += chunk_table.num_rows
                stats["data_mb"] += self.table_size_bytes(chunk_table) / (1024 * 1024)
                # print(f"[{self.node_id}] Received {chunk_table.num_rows} rows from {client['id']}")

            stats["total_time"] = stats["total_time"] + (time.time() - start_time)

        print(f"\n[{self.node_id}] Command completed.")

        stats["mbps"] = stats["data_mb"] / (stats["total_time"] == 0 and 1 or stats["total_time"])
        print(f"Rows: {stats['rows']}")
        print(f"Data MB: {stats['data_mb']:.2f}")
        print(f"Time: {stats['total_time']:.2f} seconds")
        print(f"Rate: {stats['mbps']:.2f} MB/s")

        return stats

    def close(self):
        self.client["client"].close()
        print(f"[{self.node_id}] Connection closed.")