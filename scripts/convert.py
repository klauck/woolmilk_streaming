import os
import pandas as pd
import json

# get file path
path = os.path.dirname(__file__)
data_dir = os.path.join(path, '../data/')
data_path = os.path.join(data_dir, 'data.txt')

data = {
    "Person": [],
    "Auction": [],
    "Bid": []
}

# data file is in json lines format
with open(data_path, 'r') as file:
    for line in file:
        line = line.strip()
        record = json.loads(line.strip())
        for table, content in record.items():
            if table in data:
                data[table].append(content)

for table_name, records in data.items():
    if records:
        df = pd.DataFrame(records)
        parquet_file_path = f"{data_dir}{table_name.lower()}.parquet"
        df.to_parquet(parquet_file_path)

print("Data conversion completed")