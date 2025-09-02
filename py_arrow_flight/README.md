## Installation

Optionally create a virtual environment

```
cd py_arrow_flight
python3 -m venv woolmilk
source woolmilk/bin/activate
```

Install the Python library for DataFusion and numpy

```
pip install datafusion
pip install numpy
pip install pyarrow
pip install paramiko
```

## Start local example

**1. Start the sink server**

```
python flight_server.py --server-address 127.0.0.1:8820
```

**2. Start the processing node**

```
python flight_processing_node.py --server-address 127.0.0.1:8815 --exit_node 127.0.0.1:8820
```

**3. Start the source client**

```
python flight_client.py --stream nexmark.bid --processing-servers 127.0.0.1:8815
```

## Multiple processing servers

Start multiple processing nodes on different ports

```
python flight_processing_node.py --server-address 127.0.0.1:8815 --exit_node 127.0.0.1:8820
python flight_processing_node.py --server-address 127.0.0.1:8816 --exit_node 127.0.0.1:8820
```

Client can send to multiple processing servers

```
python flight_client.py --stream nexmark.bid --processing-servers 127.0.0.1:8815,127.0.0.1:8816
```

## Query processing

Add SQL queries to filter data

```
python flight_processing_node.py --server-address 127.0.0.1:8815 --exit_node 127.0.0.1:8820 --query "SELECT * FROM nexmark_data WHERE price > 100"
```

## Command Line Options

### flight_server.py (Sink Node)

```
--server-address    Server address to bind to (default: 0.0.0.0:8820)
```

### flight_processing_node.py (Processing Node)

```
--server-address    Address to run the processing node on (default: localhost:8815)
--exit_node         Address of the exit/sink node (default: localhost:8820)
--query             SQL query to run on incoming data (default: SELECT * FROM nexmark_data)
```

### flight_client.py (Client Node)

```
--stream            Stream type: nexmark.bid, nexmark.auction, nexmark.person (default: nexmark.bid)
--tuples-per-batch  Number of records per batch (default: 10000)
--overall-tuples    Total number of records to send (default: 1000000)
--processing-servers Comma-separated list of processing servers (default: localhost:8815)
--thread-count      Number of threads for parallel sending (default: 1)
```

## Deployment

**Local deployment**

```
cd scripts
python3 deployment.py local
```

**Remote deployment**

```
cd scripts
python3 deployment.py deploy
```

### deployment.py options

```
mode                local or deploy
--config            Configuration file path (default: config.json)
--src-dir           Source directory path (default: ../src)
--log-dir           Log directory path (default: logs)
```

## Configuration (config.json)

**Server configuration**

```json
"config": {
  "servers": {
    "192.168.1.10": {
      "username": "user",
      "password": "password",
      "base_dir": "arrow-flight",
      "python_env": "/path/to/python3"
    }
  }
}
```

**Sink nodes** - receive final data

```json
"sinkNodes": [
  {"serverAddress": "192.168.1.10:8820"}
]
```

**Processing nodes** - apply queries and forward to sinks

```json
"processingNodes": [
  {
    "serverAddress": "192.168.1.11:8815",
    "query": "SELECT * FROM nexmark_data WHERE price > 100",
    "forwardNode": "192.168.1.10:8820"
  }
]
```

**Client nodes** - generate data streams

```json
"sourceNodes": [
  {
    "processingNodes": [{"address": "192.168.1.11:8815"}],
    "stream": "nexmark.bid",
    "overall_tuples": 100000,
    "tuples_per_batch": 10000,
    "thread_count": 1,
    "deployment_server": "192.168.1.12"
  }
]
```

### Config options explained

-  **username/password**: SSH credentials for remote deployment
-  **base_dir**: Directory on remote server to store files
-  **python_env**: Path to Python executable on remote server
-  **serverAddress**: Host:port where service runs
-  **query**: Optional SQL query for processing nodes
-  **sinkNode**: Where processing node forwards data
-  **deployment_server**: Where to deploy the client (required)
-  **stream**: Data stream type (nexmark.bid, nexmark.auction, nexmark.person)
-  **overall_tuples**: Total tuples to generate
-  **tuples_per_batch**: Tuples per batch
-  **thread_count**: Parallel threads for data sending
