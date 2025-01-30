# Introduction

This guide explains how to generate Nexmark data, convert it into Parquet files, and use Apache Ballista to run `SQL` queries against the data in a distributed manner.

## Nexmark

Nextmark is benchmark for continues queries. We can use [nexmark-rs](https://github.com/risingwavelabs/nexmark-rs) to generate the data.

### Installation

To generate data first we have to install the nexmark data generate binary. Which we can easily install it with the help of cargo.

```bash
cargo install nexmark --features bin
```

### Generating Data

To generate data we have to run nexmark command just like

```bash
nexmark -n 1000 --no-wait
```

This command generates a text file containing JSON objects, one per line. The nexmark consist over `Auction`, `Bid` and `Person` objects.

## Converting Data Into Parqueet

To use the data with the ballista we have to convert the data into `parquet` format.

```python
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
```

This python script will convert the data into parquet file and store the data on the disk.

## Continue Reading

[Datafusion Introduction](ballista-introduction.md)
[Ballista Introduction](ballista-introduction.md)
