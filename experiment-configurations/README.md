# experiment-configurations

WoolMilk distributed-streaming benchmarks for the thesis. Each subfolder is one run
(cluster size + dataset + tuple count). A fresh session should read this file first.

Kept in **two mirrored copies**:
- `woolmilk_streaming/experiment-configurations/` (branch `usama-ms-thesis`, code side)
- `thesis-ms/experiment-configurations/` (writing side)
Configs + topology figure live in both; result logs are copied into both after a run.

## Dataset (fixed)

nexmark **BID**, 10M tuples, 5 columns: `auction, bidder, price, channel, date_time`.
`url` + `extra` dropped (url redundant with channel + degenerate; extra random text).

Per-column intent (why the matrix exists):
- `channel` → dictionary **encoding** wins (10k distinct, dict 3.11 > zstd 2.56)
- `auction, bidder, date_time` → **compression** wins (low-card / sequential ints)
- `price` → mild compression

Channel selectivity (measured on 10M): 4 named channels (Apple/Baidu/Facebook/Google)
= 50% of rows; 10k `channel-*` = other 50%.

## Run folder contents

```
<run-id>/
  benchmark-<id>.json   generated benchmark config (source templates + node_configs + experiments)
  cluster-<id>.json     cluster deploy config (processing + sink, for run_cluster)
  topology.png          node topology figure
  run-README.md         this run's exact params + launch command
  <timestamp>_<name>/itr_<n>/   per-experiment logs (WM_LOG= JSON: send/receive times), copied here
```

Experiment folder name encodes the point:
`maxbps_<batch>k_<buffered|unbuffered>_<none|lz4|zstd>[_dict]_<all|gtH|ltB|gtz>`.

## Matrix (per run)

batch [5k,10k,25k,50k,100k] x buffering [on,off] x compression [none,lz4,zstd]
x encoding [none, dict-on-channel] x selectivity [all(100%), channel>'H'(50%),
channel<'B'(12.5%), channel>'z'(0%)] = **240 experiments**. Rate = maxbps.

## Regenerate a config (from woolmilk repo)

`python scripts/generate-config.py --config-name <id> --out-dir experiment-configurations/<id>`
(edit knobs at the top of `scripts/generate-config.py` — NODES, SETTINGS, OPT_MODES, QUERIES).
Topology figure: `python scripts/plot_topology.py --out experiment-configurations/<id>/topology.png`.

## Launch (author runs; cluster must be up)

`python -m woolmilk.benchmark --config-file experiment-configurations/<id>/benchmark-<id>.json \
  --experiment-dir experiment-configurations/<id> --mode remote`

After the run, copy the result folders into `thesis-ms/experiment-configurations/<id>/`.

## Runs

- `3node-bid-10M/` — source .80 -> processing .81 -> sink .82, 10M bid, full 240-matrix,
  iterations=1 (smoke). Bump `iterations` to 3 in the generator for the real run.
