## Installation

Optionally create a virtual environment

```
python3 -m venv woolmilk_env
source woolmilk_env/bin/activate
```

Install the requirements including DataFusion and PyArrow

```
pip install -r requirements.txt
```

## Start local example

**1. Start the sink server**

```
python sink_node.py --port 8027
```

**2. Start the processing node (in another terminal)**

```
python processing_node.py \
  --port 8017 \
  --forward-node 127.0.0.1:8027 \
  --query "SELECT * FROM nexmark_data WHERE name > 'H'" \
  --query-result-schema '{
    "fields": [
      {"name": "id", "type": "int64"},
      {"name": "name", "type": "string"},
      {"name": "email_address", "type": "string"},
      {"name": "credit_card", "type": "string"},
      {"name": "city", "type": "string"},
      {"name": "state", "type": "string"},
      {"name": "date_time", "type": "int64"},
      {"name": "extra", "type": "string"}
    ]
  }'
```

**3. Start the source client (in another terminal)**

```
python source_node.py --stream nexmark_person --processing-nodes 127.0.0.1:8017
```

## Multiple processing servers

Start multiple processing nodes on different ports

```
python processing_node.py \
  --port 8017 \
  --forward-node 127.0.0.1:8027 \
  --query "SELECT * FROM nexmark_data WHERE name > 'H'" \
  --query-result-schema '{
    "fields": [
      {"name": "id", "type": "int64"},
      {"name": "name", "type": "string"},
      {"name": "email_address", "type": "string"},
      {"name": "credit_card", "type": "string"},
      {"name": "city", "type": "string"},
      {"name": "state", "type": "string"},
      {"name": "date_time", "type": "int64"},
      {"name": "extra", "type": "string"}
    ]
  }'

python processing_node.py \
  --port 8018 \
  --forward-node 127.0.0.1:8027 \
  --query "SELECT * FROM nexmark_data WHERE name > 'H'" \
  --query-result-schema '{
    "fields": [
      {"name": "id", "type": "int64"},
      {"name": "name", "type": "string"},
      {"name": "email_address", "type": "string"},
      {"name": "credit_card", "type": "string"},
      {"name": "city", "type": "string"},
      {"name": "state", "type": "string"},
      {"name": "date_time", "type": "int64"},
      {"name": "extra", "type": "string"}
    ]
  }'
```

Clients can send to multiple processing servers

```
python source_node.py --stream nexmark_person --processing-nodes 127.0.0.1:8017,127.0.0.1:8018
```

## Command Line Options

### sink_node.py (Sink Node)

```
--port PORT                     Port to run the WoolMilk sink node
--result-folder RESULT_FOLDER   Folder to store Parquet results
```

### processing_node.py (Processing Node)

```
--port PORT                                    Port to run the WoolMilk processing node
--forward-node FORWARD_NODE                    Address of the node to forward data to (host:port)
--query QUERY                                  SQL query to run on incoming batches
--query-result-schema QUERY_RESULT_SCHEMA      JSON schema definition for the data (required)
```

### source_node.py (Source Node)

```
--stream {nexmark_bid,nexmark_auction,nexmark_person}       Stream type
--generator-executable GENERATOR_EXECUTABLE                 Executable to generate data
--overall-tuples OVERALL_TUPLES                             Total number of tuples needs to be sent.
--tuples-per-batch TUPLES_PER_BATCH                         Number of tuples per batch
--offset OFFSET                                             Offset to start data generation
--step STEP                                                 Step for next tuple to generate
--processing-nodes PROCESSING_NODES                         Flight server address (host:port,host:port)
--thread-count THREAD_COUNT                                 Number of threads to use for sending data
--store-input STORE_INPUT                                   Folder to store generated data
```

## Deployment

**Run all nodes in one command**

A configuration is specified in JSON: [config.json](https://github.com/klauck/woolmilk_streaming/blob/main/scripts/config.json)

```
python woolmilk/run_cluster.py --config scripts/config.json
```

