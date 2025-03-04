import time
import pyarrow as pa
import pyarrow.flight as fl
import pandas as pd

class FlightDataClient:
    def __init__(self, host="0.0.0.0", port=8815):
        location = f"grpc://{host}:{port}"
        self.client = fl.FlightClient(location)

    def read_all_data_for_command(self, command: str):
        descriptor = fl.FlightDescriptor.for_command(command.encode("utf-8"))
        flight_info = self.client.get_flight_info(descriptor)

        ticket = flight_info.endpoints[0].ticket

        all_batches = []
        total_rows = 0

        while True:
            reader = self.client.do_get(ticket)
            chunk_table = reader.read_all()

            if chunk_table.num_rows == 0:
                break

            total_rows += chunk_table.num_rows
            print(f"Read {total_rows} rows so far...")
            all_batches.extend(chunk_table.to_batches())

        if all_batches:
            final_schema = all_batches[0].schema
            final_table = pa.Table.from_batches(all_batches, schema=final_schema)
        else:
            final_table = pa.Table.from_batches([], schema=flight_info.schema)

        return final_table

    def run_nexmarkq1(self):
        start_time = time.time()
        q1_table = self.read_all_data_for_command("nexmarkq1")
        end_time = time.time()

        num_rows = q1_table.num_rows
        data_mb = q1_table.nbytes / (1024 * 1024)
        print("=== Nexmark Q1 ===")
        print(f"Rows: {num_rows}")
        print(f"Data size: {data_mb:.2f} MB")
        print(f"Time: {end_time - start_time:.2f} sec")
        print("=== END ===")

    def run_nexmarkq2(self):
        start_time = time.time()
        q2_table = self.read_all_data_for_command("nexmarkq2")
        end_time = time.time()

        num_rows = q2_table.num_rows
        data_mb = q2_table.nbytes / (1024 * 1024)
        print("=== Nexmark Q2 ===")
        print(f"Rows: {num_rows}")
        print(f"Data size: {data_mb:.2f} MB")
        print(f"Time: {end_time - start_time:.2f} sec")
        print("=== END ===")

    def run(self):
        overall_start = time.time()
        self.run_nexmarkq1()
        self.run_nexmarkq2()
        overall_end = time.time()
        print(f"\nTotal time for both queries: {overall_end - overall_start:.2f} sec")


if __name__ == "__main__":
    client = FlightDataClient(host="localhost", port=8815)
    client.run()
