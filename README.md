# WoolMilk Streaming

In this repository, we investigate how to run streaming queries in a multi-node cluster using [Apache DataFusion](https://datafusion.apache.org/), or more general in a composed data management system.
The idea of using DataFusion is that we can "spend most time implementing value-adding features rather than replicating existing analytic engine technologies" [1].

[Composable_Systems_for_Optimizing_Distributed_Stream_Processing.pdf](https://github.com/user-attachments/files/21215294/Composable_Systems_for_Optimizing_Distributed_Stream_Processing.pdf)


## Overview
Sketch of data transfer from streaming sources to sink(s)

![streaming_scenario](https://github.com/user-attachments/assets/7dc973de-dd1b-469e-9dbb-7b92d0764640)


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

Install the [nexmark data generator](https://github.com/risingwavelabs/nexmark-rs)

```bash
cargo install nexmark --features bin
```

## Run Tests

```
python -m unittest discover
```


## Run local example step-by-step

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
python source_node.py --stream nexmark.person --processing-nodes 127.0.0.1:8017
```

## Run local example in one command**

A configuration is specified in JSON: [config.json](https://github.com/klauck/woolmilk_streaming/blob/main/scripts/config.json)

```
python woolmilk/run_cluster.py --config scripts/config.json
```


## Resources

### Composable Data Management Systems

  - Pedreira et al.: The Composable Data Management System Manifesto
    
    https://www.vldb.org/pvldb/vol16/p2679-pedreira.pdf

  - [1] Lamb et al.: Apache Arrow DataFusion: a Fast, Embeddable, Modular Analytic Query Engine
    
    http://andrew.nerdnetworks.org/other/SIGMOD-2024-lamb.pdf
  
  - Khurana et al.: The Modern Data Architecture: The Deconstructed Database

    https://www.usenix.org/system/files/login/articles/login_winter18_08_khurana.pdf

  - Voltron Data: The Composable Codex
    
    https://voltrondata.com/codex


    #### DataFusion Streaming

    - [GitHub issue](https://github.com/apache/datafusion/issues/1544)
    - [Synnada](https://www.synnada.ai/)
    - [Arroyo](https://www.arroyo.dev/)


### Stream Processing

  - Arasu et al.: The CQL Continuous Query Language: Semantic Foundations and Query Execution

    http://infolab.stanford.edu/~arvind/papers/cql-vldbj.pdf

### Benchmarks

  - NEXMark

    [Queries](https://web.archive.org/web/20100620010601/http://datalab.cs.pdx.edu/niagaraST/NEXMark/)

  - Linear Road

    [Queries](http://infolab.stanford.edu/stream/cql-benchmark.html)
