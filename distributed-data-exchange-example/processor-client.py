import time
import pyarrow as pa
import pyarrow.flight as fl
import pandas as pd

class FlightDataClient:
    def __init__(self, host="0.0.0.0", port=8815):
        location = f"grpc://{host}:{port}"
        self.client = fl.FlightClient(location)

        # We'll store stats for all flights in this list
        self.stats = []

    def read_all_data_for_ticket(self, ticket, flight_name: str):
        chunk_count = 0

        while True:
            start_time = time.time()
            # Fetch the next chunk
            reader = self.client.do_get(ticket)
            table = reader.read_all()
            end_time = time.time()

            # If no rows, we are done for this ticket
            if table.num_rows == 0:
                break

            chunk_count += 1
            data_size_bytes = table.nbytes 

            self.stats.append({
                "name": flight_name,
                "total_time": end_time - start_time,
                "rows_fetched": table.num_rows,
                "data_size_bytes": data_size_bytes,
            })

    def run(self):
        flights = self.client.list_flights()
        
        for flight_info in flights:
            flight_name = flight_info.descriptor.command.decode("utf-8")

            if not flight_info.endpoints:
                continue

            endpoint = flight_info.endpoints[0]
            current_ticket = endpoint.ticket

            print(f"\n=== Starting to read data for flight: '{flight_name}' ===")
            self.read_all_data_for_ticket(current_ticket, flight_name)

        df = pd.DataFrame(self.stats, columns=[
            "name", "total_time", "rows_fetched", "data_size_bytes"
        ])

        df_grouped = df.groupby("name", as_index=False).sum()

        # df_grouped["data_size_MB"] = df_grouped["data_size_bytes"] / (1024 * 1024)
        df_grouped["data_size_GB"] = df_grouped["data_size_bytes"] / (1024 * 1024 * 1024)
        df_grouped["data_in_gb_per_sec"] = df_grouped["data_size_GB"] / df_grouped["total_time"]
        
        print("\n=== Combined Stats ===")
        print(df_grouped.to_string())

if __name__ == "__main__":
    client = FlightDataClient(host="localhost", port=8815)
    client.run()
