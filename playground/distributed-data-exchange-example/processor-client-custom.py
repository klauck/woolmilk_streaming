import time
import pyarrow as pa
import pyarrow.flight as fl
import pandas as pd

class FlightDataClient:
    def __init__(self, server_list):
        self.server_list = server_list

    def read_all_data_for_command(self, host, port, command: str):
        location = f"grpc://{host}:{port}"
        client = fl.FlightClient(location)

        descriptor = fl.FlightDescriptor.for_command(command.encode("utf-8"))
        flight_info = client.get_flight_info(descriptor)

        ticket = flight_info.endpoints[0].ticket

        all_batches = []
        total_rows = 0

        while True:
            reader = client.do_get(ticket)
            chunk_table = reader.read_all()

            if chunk_table.num_rows == 0:
                break

            total_rows += chunk_table.num_rows
            all_batches.extend(chunk_table.to_batches())
            print(f"Read {total_rows} rows so far...")

        if all_batches:
            final_schema = all_batches[0].schema
            final_table = pa.Table.from_batches(all_batches, schema=final_schema)
        else:
            final_table = pa.Table.from_batches([], schema=flight_info.schema)

        return final_table

    def run_nexmarkq1(self, host, port):
        start_time = time.time()
        q1_table = self.read_all_data_for_command(host, port, "nexmarkq1")
        end_time = time.time()

        num_rows = q1_table.num_rows
        data_mb = q1_table.nbytes / (1024 * 1024)
        total_sec = end_time - start_time
        data_rate = data_mb / total_sec if total_sec > 0 else 0
        print(f"=== Nexmark Q1 on {host}:{port} ===")
        print(f"Rows: {num_rows}")
        print(f"Data size: {data_mb:.2f} MB")
        print(f"Data rate: {data_rate:.2f} MB/sec")
        print(f"Time: {total_sec:.2f} sec")
        print("=== END ===\n")
        return (num_rows, data_mb, end_time - start_time)

    def run_nexmarkq2(self, host, port):
        start_time = time.time()
        q2_table = self.read_all_data_for_command(host, port, "nexmarkq2")
        end_time = time.time()

        num_rows = q2_table.num_rows
        data_mb = q2_table.nbytes / (1024 * 1024)
        total_sec = end_time - start_time
        data_rate = data_mb / total_sec if total_sec > 0 else 0
        print(f"=== Nexmark Q2 on {host}:{port} ===")
        print(f"Rows: {num_rows}")
        print(f"Data size: {data_mb:.2f} MB")
        print(f"Time: {end_time - start_time:.2f} sec")
        print(f"Data rate: {data_rate:.2f} MB/sec")
        print("=== END ===\n")
        return (num_rows, data_mb, total_sec)

    def run(self):
        start = time.time()
        for (host, port) in self.server_list:
            print(f"--- Connecting to {host}:{port} ---\n")
            self.run_nexmarkq1(host, port)
            self.run_nexmarkq2(host, port)

        end = time.time()
        print(f"=== Finished all servers in {end - start:.2f} sec ===")

if __name__ == "__main__":
    servers = [
        ("localhost", 8815),
        ("localhost", 8816)
    ]
    client = FlightDataClient(servers)
    client.run()
