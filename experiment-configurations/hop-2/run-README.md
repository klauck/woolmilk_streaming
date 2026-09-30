# Run: hop-2 (source -> processing -> sink, 2 hops)

Two-hop pipeline: source -> processing -> sink.
all on switch B (intra-switch). Compression sweep + 4 queries via the processing node.

## Topology (3 nodes, all switch B = intra-switch)
```
[source .87:8007]  -->  [processing .88:8017]  -->  [sink .89:8027]
```
- source->proc and proc->sink both LOCAL (switch B, no uplink)

## Data
- schema: no_extra (6 col) = auction,bidder,price,channel,url,date_time
- 10M tuples/source (~1.12 GB raw)
- source .87 needs `nexmark_bid_10000000_0_1.parquet` in woolmilk/input_data/
  (.87 was a proc before -> NO source data yet; copy the 10M no_extra file there)

## Fixed
- proc: FILTER only (no projection), keeps 6 col; proc compression = source codec
- NO encoding

## Swept (72 experiments)
| dim | values |
|---|---|
| compression (3) | none, lz4, zstd |
| buffering (2) | on, off |
| batch (3) | 10k, 25k, 50k  (all maxbps) |
| queries / selectivity (4) | all(100%) · channel>'H'(50%) · channel<'B'(12.5%) · channel>'z'(0%) |

72 experiments x 3 iterations = 216 runs.
name: `maxbps_<raw|lz4|zstd>_<batch>k_<buffered|unbuffered>_<none|comp>_<all|gtH|ltB|gtz>`
(visualizer: query chips = selectivity; codec shown on the bps/batch x-axis)

## Prerequisites
1. Deploy the 3 nodes:
   `ansible-playbook -i experiment-configurations/hop-2/inventory.ini scripts/deployment.yml -K -e "ansible_python_interpreter=/usr/bin/python3"`
2. Copy the 10M no_extra file to source .87 (was a proc before, has no source data):
   `nexmark_bid_10000000_0_1.parquet` -> .87:~/usama/woolmilk_streaming/woolmilk/input_data/

## Launch (duck)
```
python -m woolmilk.run_cluster --config experiment-configurations/hop-2/cluster-hop-2.json --mode remote
python -m woolmilk.benchmark --config-file experiment-configurations/hop-2/benchmark-hop-2.json --experiment-dir experiment-configurations/hop-2/results --mode remote
```

## Regenerate
`python local-scripts/generate_hop2_config.py --out-dir experiment-configurations/hop-2`
(local-scripts gitignored; edit SRC/PROC/SINK/COMPRESSIONS/BATCHES/SELECTIVITY there.)
