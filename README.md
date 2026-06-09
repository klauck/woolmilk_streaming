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

Install WoolMilk in editable mode to enable package-style imports during development
```
pip install -e .
```

Install the [nexmark data generator](https://github.com/risingwavelabs/nexmark-rs)

```bash
cargo install nexmark --features bin
```

## Run Tests

```
python -m unittest discover
```


## Run Local Example Step-by-step

**1. Start the sink server**

```
python woolmilk/sink_node.py --port 8027
```

**2. Start the processing node (in another terminal)**

```
python woolmilk/processing_node.py \
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
python woolmilk/source_node.py --stream nexmark_person --processing-nodes 127.0.0.1:8017
```

## Run Local Example in One Command

A configuration is specified in JSON: [config.json](https://github.com/klauck/woolmilk_streaming/blob/main/scripts/config.json)

```
python woolmilk/run_cluster.py --config scripts/config.json
```

## Docker Setup for Remote Execution Emulation

You can run multi-procross experiements locally without Docker.
However, to test remote deployment, logging, and result collection via SSH, you can use the provided Docker-based environment to mimic remote execution.

### Build and start docker environment

To build and start the Docker container, run:

```bash
cd scripts
./docker-start.sh
cd ..
```

The `docker-start.sh script` will:

1. Build the Docker image using the [Dockerfile](./woolmilk/Dockerfile)

2. Start the containers using the Docker Compose configuration [file](./woolmilk/docker-compose.yml)

### Running a remote example

Once the container is running, execute:

```bash
python woolmilk/run_cluster.py \
  --config scripts/docker-config-remote.json \
  --mode remote \
  --log-to-file \
  --local-log-dir ./local-logs \
  --local-results-dir ./results
```

This command launches all nodes inside Docker containers and collects logs and results in the `./local-logs` and `./results` directories respectively.

### Running remote tests

Docker-based tests are disabled by default, including in the GitHub workflow.

To enable and run tests using the Docker setup:

1. Build and start the Docker container (as shown above).

2. Run:

```bash
WOOLMILK_DOCKER=true python -m unittest discover
```

### Stopping docker containers

To stop and remove the running containers, run:

```bash
cd woolmilk
docker compose down
cd ..
```

## Ansible Deployment for Distributed Execution

See [WoolMilk Ansible Deployment](https://github.com/klauck/woolmilk_streaming/blob/main/docs/woolmilk_ansible_deployment.md)



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
