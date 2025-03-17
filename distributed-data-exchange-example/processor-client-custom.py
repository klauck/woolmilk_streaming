import time
import pyarrow as pa
import pyarrow.flight as fl
import pandas as pd
import json
from pyarrow._flight import Ticket

class FlightDataClient:
    def __init__(self, host, port, exit_host, exit_port, node_id=""):
        self.host = host
        self.port = port
        self.node_id = node_id
        location = f"grpc://{host}:{port}"
        self.client = fl.FlightClient(location)
        exit_location = f"grpc://{exit_host}:{exit_port}"
        self.exit_client = fl.FlightClient(exit_location)
        print(f"Connected to {location}")

    def read_all_data_for_command(self, command: str):
        descriptor = fl.FlightDescriptor.for_command(command.encode("utf-8"))
        flight_info = self.client.get_flight_info(descriptor)
        ticket = flight_info.endpoints[0].ticket

        all_batches = []
        total_rows = 0

        config = {
            "node_id": self.node_id,
            "command": command,
        }
        config_str = json.dumps(config)
        new_ticket = Ticket(config_str)

        descriptor = fl.FlightDescriptor.for_command(command)

        while True:
            reader = self.client.do_get(new_ticket)
            chunk_table = reader.read_all()

            if chunk_table.num_rows == 0:
                break

            writer, _ = self.exit_client.do_put(descriptor, chunk_table.schema)
            writer.write_table(chunk_table)
            writer.close()

            total_rows += chunk_table.num_rows
            all_batches.extend(chunk_table.to_batches())
            print(f"Read {total_rows} rows so far...")

        if all_batches:
            final_schema = all_batches[0].schema
            final_table = pa.Table.from_batches(all_batches, schema=final_schema)
        else:
            final_table = pa.Table.from_batches([], schema=flight_info.schema)

        return final_table

    def run_command(self, command: str):
        start_time = time.time()

        q_table = self.read_all_data_for_command(command)

        end_time = time.time()

        num_rows = q_table.num_rows
        data_mb = q_table.nbytes / (1024 * 1024)
        total_sec = end_time - start_time
        data_rate = data_mb / total_sec if total_sec > 0 else 0

        print(f"=== {command} on {self.host}:{self.port} ===")
        print(f"Rows: {num_rows}")
        print(f"Data size: {data_mb:.2f} MB")
        print(f"Data rate: {data_rate:.2f} MB/sec")
        print(f"Time: {total_sec:.2f} sec")
        print("=== END ===\n")
        return num_rows, data_mb, total_sec

    def run_all_commands(self):
        commands = ["nexmarkq1", "nexmarkq2"]

        for command in commands:
            self.run_command(command)

if __name__ == "__main__":
    host = "localhost"
    port = 8815
    exit_host = "localhost"
    exit_port = 8820
    client1 = FlightDataClient(host, port, exit_host, exit_port, "node_1")
    client2 = FlightDataClient(host, port, exit_host, exit_port, "node_2")
    print("======NODE 1======\n")
    client1.run_all_commands()
    print("======NODE 2======\n")
    client2.run_all_commands()
