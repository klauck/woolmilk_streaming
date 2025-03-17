import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import os

def duplicate_parquet_file(input_file: str, output_file: str, times: int):
    df = pd.read_parquet(input_file)
    
    df_combined = pd.concat([df] * times, ignore_index=True)
    
    df_combined.to_parquet(output_file, engine='pyarrow', index=False)
    
    print(f"Saved {output_file} with {len(df_combined)} rows.")

x_times = 10
node_index = 1
current_file_dir = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.join(current_file_dir, "..", "data")
files = {
    os.path.join(data_dir, "bid.parquet"): os.path.join(data_dir, f"bid_{node_index}.parquet"),
    os.path.join(data_dir, "person.parquet"): os.path.join(data_dir, f"person_{node_index}.parquet"),
    os.path.join(data_dir, "auction.parquet"): os.path.join(data_dir, f"auction_{node_index}.parquet"),
    os.path.join(data_dir, "category.parquet"): os.path.join(data_dir, f"category_{node_index}.parquet"),
}

for input_file, output_file in files.items():
    duplicate_parquet_file(input_file, output_file, x_times)