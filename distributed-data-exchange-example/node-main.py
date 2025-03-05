import os
import threading
from node_server_datafusion import NodeFlightServer  # import your NodeFlightServer from wherever you defined it

def run_server(parquet_files, port):
    server = NodeFlightServer(parquet_registrations=parquet_files, host="0.0.0.0", port=port)
    print(f"Serving Flight on 0.0.0.0:{port}")
    server.serve()

if __name__ == "__main__":
    current_file_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(current_file_dir, "..", "data")

    node_index_1 = 1
    parquet_files_1 = {
        "bids":     os.path.join(data_dir, f"bid_{node_index_1}.parquet"),
        "auctions": os.path.join(data_dir, f"auction_{node_index_1}.parquet"),
        "persons":  os.path.join(data_dir, f"person_{node_index_1}.parquet"),
    }

    node_index_2 = 2
    parquet_files_2 = {
        "bids":     os.path.join(data_dir, f"bid_{node_index_2}.parquet"),
        "auctions": os.path.join(data_dir, f"auction_{node_index_2}.parquet"),
        "persons":  os.path.join(data_dir, f"person_{node_index_2}.parquet"),
    }

    port1 = 8815
    port2 = 8816

    t1 = threading.Thread(target=run_server, args=(parquet_files_1, port1), daemon=True)
    t2 = threading.Thread(target=run_server, args=(parquet_files_2, port2), daemon=True)

    t1.start()
    t2.start()

    t1.join()
    t2.join()
