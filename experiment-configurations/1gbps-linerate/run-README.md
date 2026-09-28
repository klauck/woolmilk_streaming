# Run: 1gbps-linerate (final 20-node, Stefan .5 + .5 = 1 Gbps)

The scale-up of base-1/base-2. Topology-aware fan-in: raw source->proc stays intra-switch,
only the reduced proc->sink stream crosses one uplink. At 50% selectivity (gtH) each proc
emits ~0.5 Gbps -> two procs saturate a sink's 1 GbE NIC.

## Topology (14 nodes: 8 src / 4 proc / 2 sink)
```
 LEFT -> SINK 88                          RIGHT -> SINK 98
 80,81 -> proc 82 --\                     90,91 -> proc 92 --\
 85,86 -> proc 87 --> SINK 88             95,96 -> proc 97 --> SINK 98
          (0.5 + 0.5 = 1 Gbps)                    (0.5 + 0.5 = 1 Gbps)
```
- source->proc: intra-switch (raw stays local); proc->sink crosses A->B / C->D uplink
- 2 procs per sink; halves independent (B<->C middle unused)

## Treatment (matches base-1/base-2)
- source: no_extra 6-col (auction,bidder,price,channel,url,date_time), compression + dict(channel)
- proc: FILTER on channel, then PROJECT channel OUT -> 5-col (auction,bidder,price,url,date_time)
        proc compression = source codec, no dict
- data: 10M tuples/source (80M total, ~1.12 GB/source raw)
- iterations: 2

## Swept (96 experiments)
| dim | values |
|---|---|
| compression x encoding (4) | none/raw · dict/raw · zstd · zstd+dict  (lz4 dropped: slow) |
| buffering (2) | on, off |
| batch (3) | 25k, 50k, 100k (all maxbps) |
| selectivity (4) | all · channel>'H' (50% = design point) · channel<'B' (12.5%) · channel>'z' (0%) |

96 experiments x 2 iterations = 192 runs.
name: `maxbps_<batch>k_<buffered|unbuffered>_<none|enc|comp|comp_enc>_<all|gtH|ltB|gtz>`

## Prerequisites
1. Deploy the 14 nodes:
   `ansible-playbook -i experiment-configurations/1gbps-linerate/inventory.ini scripts/deployment.yml -K -e "ansible_python_interpreter=/usr/bin/python3"`
2. 10M no_extra (6-col, WITH url) parquet on EACH of the 8 source Pis, named to the cache:
   `nexmark_bid_10000000_0_1.parquet` in `woolmilk/input_data/`.
   NOTE: the old 10M files on the Pis are 5-col (no url) — must overwrite with the new 6-col file.
   Generate: `local-scripts/generate_bid_variants.py` style, COLUMNS = no_extra 6-col, ROWS = 10M.
   Copy to ALL 8 sources (80 81 85 86 90 91 95 96):
   `for ip in 80 81 85 86 90 91 95 96; do scp -J duck data/linerate/nexmark_bid_10000000_0_1.parquet picocluster@192.168.2.$ip:~/usama/woolmilk_streaming/woolmilk/input_data/; done`
   (all sources share the same 10M file; duplicate data is fine for a throughput run.)

## Launch (duck, detached)
```
setsid nohup .venv/bin/python -u -m woolmilk.run_cluster --config experiment-configurations/1gbps-linerate/cluster-1gbps-linerate.json --mode remote > linerate_cluster.log 2>&1 < /dev/null &
setsid nohup .venv/bin/python -u -m woolmilk.benchmark --config-file experiment-configurations/1gbps-linerate/benchmark-1gbps-linerate.json --experiment-dir experiment-configurations/1gbps-linerate/results --mode remote > linerate_benchmark.log 2>&1 < /dev/null &
```

## Regenerate
`python local-scripts/generate_linerate_config.py --out-dir experiment-configurations/1gbps-linerate`
(local-scripts gitignored; edit SRC/PROC/SINK/COMP_ENC/BATCHES/SELECTIVITY there.)
