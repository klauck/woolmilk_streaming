# Run: hop-1 (source -> sink, compression sweep)

Single hop: source streams straight to the sink (Flight do_put). NO processing node,
NO encoding. Pure compression comparison over batch size + buffering.

## Topology (2 nodes)
```
[source .80:8007]  --Flight (compress)-->  [sink .81:8027]
   switch A                                    switch A  (intra-switch, local)
```
- source.processing_nodes = [sink]; cluster: sink only, processing_nodes = []

## Data
- schema: no_extra (6 col) = auction,bidder,price,channel,url,date_time
- 10M tuples/source (~1.12 GB raw)
- .80 already has `nexmark_bid_10000000_0_1.parquet` (from the linerate run) -> cache hit, no prep

## Swept (18 experiments)
| dim | values |
|---|---|
| compression (3) | none, lz4, zstd |
| buffering (2) | on, off |
| batch (3) | 10k, 25k, 50k  (all maxbps) |

NO encoding, NO selectivity (sink can't filter).
18 experiments x 3 iterations = 54 runs.
name: `maxbps_<batch>k_<buffered|unbuffered>_<none|comp>_<raw|lz4|zstd>`

## Prerequisites
1. Deploy the 2 nodes:
   `ansible-playbook -i experiment-configurations/hop-1/inventory.ini scripts/deployment.yml -K -e "ansible_python_interpreter=/usr/bin/python3"`
2. Data already on .80 (linerate's 10M no_extra file). No copy needed.

## Launch (duck)
```
python -m woolmilk.run_cluster --config experiment-configurations/hop-1/cluster-hop-1.json --mode remote
python -m woolmilk.benchmark --config-file experiment-configurations/hop-1/benchmark-hop-1.json --experiment-dir experiment-configurations/hop-1/results --mode remote
```

## Regenerate
`python local-scripts/generate_hop1_config.py --out-dir experiment-configurations/hop-1`
(local-scripts gitignored; edit SRC/SINK/COMPRESSIONS/BATCHES there.)
