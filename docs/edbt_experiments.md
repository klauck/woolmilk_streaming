# Reproducing the EDBT 2027 Experiments

This document describes how to reproduce the experiments of our EDBT 2027 short paper
"WoolMilk: Distributed Dataflow Research for Continuous Queries Using Open Data
Management Technologies". Each section maps a figure of the paper to the commands
that produce it.

## Overview

| Figure | Experiment | Section in paper | Script / config |
|---|---|---|---|
| Fig. 3 | Arrow compression ratio and speed | 4.2.1 | `playground/compression_benchmark.py` |
| Fig. 4 | Single-hop Arrow Flight transfer (iPerf3 baseline, uncompressed, LZ4, Zstd) | 4.2.2 | example below |
| Fig. 5 | Two-hop transfer with DataFusion filter query (100% selectivity) | 4.3.1 | example below |
| Fig. 7 | Scaling topologies 1→1→1, 2→2→1, 4→4→1 with varying selectivity | 4.3.2 | example below |

## 1. Setup

### Hardware (as used in the paper)

- Up to nine Raspberry Pi 4 nodes (Broadcom BCM2711, quad-core Cortex-A72 @ 1.8 GHz,
  8 GB RAM, Gigabit Ethernet)
- Debian GNU/Linux 13 (trixie)

All experiments except Fig. 7 require two (Fig. 4) or three (Fig. 5) nodes. The
functional parts can also be run on a single machine (macOS or Linux) using localhost;
absolute numbers then differ from the paper.

### Software

| Component | Version |
|---|---|
| Python | 3.13.5 |
| PyArrow | 24.0.0 |
| DataFusion (Python) | 53.0.0 |
| iPerf3 | 3.18 |

```bash
python3 -m venv woolmilk_env
source woolmilk_env/bin/activate
pip install -r requirements.txt
pip install -e .
cargo install nexmark --features bin
```

### Cluster deployment

Nodes are deployed with Ansible, see
[woolmilk_ansible_deployment.md](/docs/woolmilk_ansible_deployment.md).

## 2. Data

We use Nexmark `Bid` tuples generated with RisingWave Labs' generator
(https://github.com/risingwavelabs/nexmark-rs). Full schema:

```
(auction int64, bidder int64, price int64, channel string, url string,
 date_time int64, extra string)
```

The paper evaluates three projections:

| Projection | Columns | Avg. tuple size (Arrow) | File Generation |
|---|---|---|---|
| `bid` | all columns | 184 bytes | set `ROWS = 5_000_000` and `COLUMNS = ["auction", "bidder", "price", "channel", "url", "date_time", "extra"]`, and run `python playground/generate_bids.py` |
| `bid w/o extra` | without `extra` | 112 bytes | set `ROWS = 10_000_000` and `COLUMNS = ["auction", "bidder", "price", "channel", "url", "date_time"]`, and run `python playground/generate_bids.py` |
| `bid w/o extra, url` | without `extra` and `url` | 45 bytes | set `ROWS = 20_000_000` and `COLUMNS = ["auction", "bidder", "price", "channel", "date_time"]`, and run `python playground/generate_bids.py` |

`bid w/o extra` is used for Figures 4, 5, and 7.

## 3. Methodology
- Each source sends 10 million tuples per run; we report the average of three runs.
- Throughput = uncompressed input bytes / run duration.
- Compression: Arrow's default level 1 for LZ4 (`lz4_frame`) and Zstd.
- Batch size: 10,000 tuples (Fig. 3 also 50,000).

## 4. Experiments

### Figure 3: Arrow compression performance (Section 4.2.1)

Single-threaded compression of the Arrow IPC representation of each batch, for
three projections, LZ4 and Zstd (level 1), and batch sizes of 10k and 50k tuples.
Note, the compression speed depends on the Hardware.

```bash
for f in nexmark_bid_5000000_0_1 nexmark_bid_10000000_0_1 nexmark_bid_20000000_0_1; do
  for bs in 10000 50000; do
    python playground/compression_benchmark.py \
      --input-file $f.parquet \
      --max_level=1 --batch_size=$bs
  done
done
```

### Figure 4: Single-hop Arrow Flight transfer (Section 4.2.2)

Two Raspberry Pis: one source, one sink.

iPerf3 baseline (three runs of 10 s):

```bash
# on the receiver
iperf3 -s
# on the sender
iperf3 -c <receiver-ip> -t 10
```

Arrow Flight transfer, uncompressed, LZ4, and Zstd:

Example config
```json
{
   "config": {
      "remote_servers": {
         "192.168.2.80": {
            "username": "picocluster",
            "base_dir": "/home/picocluster/halfpap/woolmilk_streaming/woolmilk",
            "python_env": "/home/picocluster/halfpap/woolmilk_streaming/venv"
         },
         "192.168.2.81": {
            "username": "picocluster",
            "base_dir": "/home/picocluster/halfpap/woolmilk_streaming/woolmilk",
            "python_env": "/home/picocluster/halfpap/woolmilk_streaming/venv"
         }
      }
   },
   "sink_nodes": [
      {
         "server_address": "192.168.2.81:8027",
         "result_folder": "results"
      }
   ],
   "processing_nodes": []
}
```
Example benchmark config (excerpt, one variant):
```json
"source_nodes": [{
   "processing_nodes": ["<sink>:8027"],
   "stream": "nexmark_bid",
   "overall_tuples": 10000000,
   "tuples_per_batch": 10000,
   "server_address": "<source>:8007",
   "input_folder": "input_data"
}],
"node_configs": {
   "maxbps_10k_buffered_comp_lz4": {
      "<source>:8007": {
         "compression": "lz4",
         "tuples_per_batch": 10000,
         "use_buffering": true
      }
   }
},
"experiments": [{
   "name": "maxbps_10k_buffered_comp_lz4",
   "node_config": "maxbps_10k_buffered_comp_lz4",
   "included_nodes": [0],
   "iterations": 3
}],
"cluster_nodes": [{"node_type": "sink", "server_address": "<sink>:8027"}]
```

### Figure 5: Two-hop transfer with DataFusion query processing (Section 4.3.1)

Three Raspberry Pis: source → processing node → sink. The processing node executes
a filter query with 100% selectivity on `bid w/o extra`.

Example config
```json
{
   "config": {
      "remote_servers": {
         "192.168.2.87": {
            "username": "picocluster",
            "base_dir": "/home/picocluster/halfpap/woolmilk_streaming/woolmilk",
            "python_env": "/home/picocluster/halfpap/woolmilk_streaming/venv"
         },
         "192.168.2.88": {
            "username": "picocluster",
            "base_dir": "/home/picocluster/halfpap/woolmilk_streaming/woolmilk",
            "python_env": "/home/picocluster/halfpap/woolmilk_streaming/venv"
         },
         "192.168.2.89": {
            "username": "picocluster",
            "base_dir": "/home/picocluster/halfpap/woolmilk_streaming/woolmilk",
            "python_env": "/home/picocluster/halfpap/woolmilk_streaming/venv"
         }
      }
   },
   "sink_nodes": [
      {
         "server_address": "192.168.2.89:8027",
         "result_folder": "results"
      }
   ],
   "processing_nodes": [
      {
         "server_address": "192.168.2.88:8017",
         "forward_node": "192.168.2.89:8027"
      }
   ]
}
```

### Figure 7: Scaling experiments (Section 4.3.2)

Topologies: n ∈ {1, 2, 4} sources, each sending to a dedicated processing node; all
processing nodes forward to a single sink (n→n→1). Filter selectivities: 100%, 50%,
12.5%, 0%. Compression: uncompressed, LZ4, Zstd.

Example config
```json
{
   "config": {
      "remote_servers": {
         "192.168.2.80": {
            "username": "picocluster",
            "base_dir": "/home/picocluster/halfpap/woolmilk_streaming/woolmilk",
            "python_env": "/home/picocluster/halfpap/woolmilk_streaming/venv"
         },
         "192.168.2.81": {
            "username": "picocluster",
            "base_dir": "/home/picocluster/halfpap/woolmilk_streaming/woolmilk",
            "python_env": "/home/picocluster/halfpap/woolmilk_streaming/venv"
         },
         "192.168.2.82": {
            "username": "picocluster",
            "base_dir": "/home/picocluster/halfpap/woolmilk_streaming/woolmilk",
            "python_env": "/home/picocluster/halfpap/woolmilk_streaming/venv"
         },
         "192.168.2.83": {
            "username": "picocluster",
            "base_dir": "/home/picocluster/halfpap/woolmilk_streaming/woolmilk",
            "python_env": "/home/picocluster/halfpap/woolmilk_streaming/venv"
         },
         "192.168.2.85": {
            "username": "picocluster",
            "base_dir": "/home/picocluster/halfpap/woolmilk_streaming/woolmilk",
            "python_env": "/home/picocluster/halfpap/woolmilk_streaming/venv"
         },
         "192.168.2.86": {
            "username": "picocluster",
            "base_dir": "/home/picocluster/halfpap/woolmilk_streaming/woolmilk",
            "python_env": "/home/picocluster/halfpap/woolmilk_streaming/venv"
         },
         "192.168.2.87": {
            "username": "picocluster",
            "base_dir": "/home/picocluster/halfpap/woolmilk_streaming/woolmilk",
            "python_env": "/home/picocluster/halfpap/woolmilk_streaming/venv"
         },
         "192.168.2.88": {
            "username": "picocluster",
            "base_dir": "/home/picocluster/halfpap/woolmilk_streaming/woolmilk",
            "python_env": "/home/picocluster/halfpap/woolmilk_streaming/venv"
         },
         "192.168.2.89": {
            "username": "picocluster",
            "base_dir": "/home/picocluster/halfpap/woolmilk_streaming/woolmilk",
            "python_env": "/home/picocluster/halfpap/woolmilk_streaming/venv"
         }
      }
   },
   "sink_nodes": [
      {
         "server_address": "192.168.2.89:8027",
         "result_folder": "results"
      }
   ],
   "processing_nodes": [
      {
         "server_address": "192.168.2.86:8017",
         "forward_node": "192.168.2.89:8027"
      },
      {
         "server_address": "192.168.2.88:8017",
         "forward_node": "192.168.2.89:8027"
      },
      {
         "server_address": "192.168.2.81:8017",
         "forward_node": "192.168.2.89:8027"
      },
      {
         "server_address": "192.168.2.83:8017",
         "forward_node": "192.168.2.89:8027"
      }
   ]
}
```
