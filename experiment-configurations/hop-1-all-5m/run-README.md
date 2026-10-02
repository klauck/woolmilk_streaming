# Run: hop-1-all-5m (source -> sink, compression sweep, full 7-col)

Single hop: source streams straight to the sink (Flight do_put). NO processing node,
NO encoding, NO buffering (source does not buffer). Pure compression comparison over
batch size. Companion to `hop-1` but with the FULL bid schema at 5M tuples.

## Topology (2 nodes)
```
[source .80:8007]  --Flight (compress)-->  [sink .81:8027]
   switch A                                    switch A  (intra-switch, local)
```
- source.processing_nodes = [sink]; cluster: sink only, processing_nodes = []

## Data
- schema: all (7 col) = auction,bidder,price,channel,url,date_time,extra
- 5M tuples/source
- .80 needs `nexmark_bid_5000000_0_1.parquet` with the full 7-col schema (NEW gen; copy over).

## Swept (9 experiments)
| dim | values |
|---|---|
| compression (3) | none, lz4, zstd |
| batch (3) | 10k, 25k, 50k  (all maxbps) |

NO buffering, NO encoding, NO selectivity (sink can't filter).
9 experiments x 3 iterations = 27 runs.
name: `maxbps_<batch>k_<none|comp>_<raw|lz4|zstd>`

## Prerequisites
1. Deploy the 2 nodes:
   `ansible-playbook -i experiment-configurations/hop-1-all-5m/inventory.ini scripts/deployment.yml -K -e "ansible_python_interpreter=/usr/bin/python3"`
2. Put the 5M full 7-col parquet on .80 as `input_data/nexmark_bid_5000000_0_1.parquet`.

## Launch (duck)
```
python -m woolmilk.run_cluster --config experiment-configurations/hop-1-all-5m/cluster-hop-1-all-5m.json --mode remote
python -m woolmilk.benchmark --config-file experiment-configurations/hop-1-all-5m/benchmark-hop-1-all-5m.json --experiment-dir experiment-configurations/hop-1-all-5m/results --mode remote
```

## Regenerate
`python local-scripts/generate_hop1all_config.py --out-dir experiment-configurations/hop-1-all-5m`
(local-scripts gitignored; edit OVERALL_TUPLES/COMPRESSIONS/BATCHES there.)
