# Run: hop-2-noextra-nourl-20m (source -> processing -> sink, filter + compression sweep, 5-col)

Two hops: source -> processing (FILTER only, keeps 5 col) -> sink. NO encoding.
Compression sweep x buffering x batch x 4 selectivity queries. Companion to `hop-2`
but with the slim 5-col schema at 20M tuples.

## Topology (3 nodes, all switch B = intra-switch)
```
[source .87:8007] --Flight--> [proc .88:8017] --Flight--> [sink .89:8027]
```
- proc: FILTER only (no projection), keeps 5 col; proc compression = source codec

## Data
- schema: no url + no extra (5 col) = auction,bidder,price,channel,date_time
- 20M tuples/source
- .87 needs `nexmark_bid_20000000_0_1.parquet` with the 5-col schema (copy over).

## Swept (72 experiments)
| dim | values |
|---|---|
| compression (3) | none, lz4, zstd |
| buffering (2) | on, off |
| batch (3) | 10k, 25k, 50k  (all maxbps) |
| queries / selectivity (4) | all(100%) · channel>'H'(50%) · channel<'B'(12.5%) · channel>'z'(0%) |

NO encoding. 72 experiments x 3 iterations = 216 runs.
name: `maxbps_<raw|lz4|zstd>_<batch>k_<buffered|unbuffered>_<none|comp>_<all|gtH|ltB|gtz>`

## Prerequisites
1. Deploy the 3 nodes:
   `ansible-playbook -i experiment-configurations/hop-2-noextra-nourl-20m/inventory.ini scripts/deployment.yml -K -e "ansible_python_interpreter=/usr/bin/python3"`
2. Put the 20M 5-col parquet on .87 as `input_data/nexmark_bid_20000000_0_1.parquet`.

## Launch (duck)
```
python -m woolmilk.run_cluster --config experiment-configurations/hop-2-noextra-nourl-20m/cluster-hop-2-noextra-nourl-20m.json --mode remote
python -m woolmilk.benchmark --config-file experiment-configurations/hop-2-noextra-nourl-20m/benchmark-hop-2-noextra-nourl-20m.json --experiment-dir experiment-configurations/hop-2-noextra-nourl-20m/results --mode remote
```

## Regenerate
`python local-scripts/generate_hop2var_config.py --config-name hop-2-noextra-nourl-20m --out-dir experiment-configurations/hop-2-noextra-nourl-20m --tuples 20000000 --schema noextra_nourl`
(local-scripts gitignored.)
