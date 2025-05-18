

import datafusion
from queries import nexmark_queries
import pyarrow.parquet as pq
from server import GroupNodeFlightServer
import threading
import time
from processor import ProcessorNode
import pandas as pd

queries = ["select * from bid"]
read_types = ["disk", "memory"]
bit_rates = [10000, 100000, 100000, 1000000]
host = "127.0.0.1"
port = 8815
no_times = 2

entry_endpoint = {
    "host": host,
    "port": port
}

pq_files = {
    "bid": "/Users/usamabintariq/Documents/GitHub/woolmilk_streaming/data/bids.parquet",
    "auction": "/Users/usamabintariq/Documents/GitHub/woolmilk_streaming/data/auctions.parquet",
    "person": "/Users/usamabintariq/Documents/GitHub/woolmilk_streaming/data/persons.parquet",
    "category": "/Users/usamabintariq/Documents/GitHub/woolmilk_streaming/data/category.parquet"
}

def main():
    columns = ["test", "bit_rate", "query", "read_type", "no_rows", "data_mb", "transfer_rate", "total_time"]
    df = pd.DataFrame(columns=columns)
    for no in range(no_times):
        for read_type in read_types:
            for br in bit_rates:
                server = GroupNodeFlightServer(
                    parquet_registrations=pq_files,
                    host=host,
                    port=port,
                    read_type=read_type,
                    chunk_size=br,
                )

                print(f"======= [{no+1}] Serving Flight on {host}:{port} with bit rate `{br}` and read type `{read_type}` =======")

                # server.serve in a separate thread
                server_thread = threading.Thread(target=server.serve)
                server_thread.start()

                print("=> Waiting for server to start...")

                # wait for server to start
                time.sleep(2)

                for i in range(len(queries)):
                    print(f"=> Running query: {i+1}")
                    processor = ProcessorNode(node_id="node_1", entry_endpoint=entry_endpoint)
                    stats = processor.run_command(queries[i])

                    pd_data = {
                        "test": no,
                        "bit_rate": br,
                        "query": i+1,
                        "read_type": read_type,
                        "no_rows": stats["rows"],
                        "data_mb": round(stats["data_mb"], 2),
                        "transfer_rate": round(stats['mbps'], 2),
                        "total_time": round(stats["total_time"], 2)
                    }

                    df = pd.concat([df, pd.DataFrame([pd_data])], ignore_index=True)

                    print(f"=> Query {i+1} completed.")
                
                server.shutdown()
                server_thread.join()
    
    print("Completed all queries.")
    # save df to csv
    df.to_csv(f"nexmark_results_new.csv", index=False)


if __name__ == "__main__":
    main()