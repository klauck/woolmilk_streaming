# Run: base-2 (source -> processing -> sink)

Adds the processing hop to base-1. Source streams to a processing node that FILTERS
(selectivity on channel) and forwards to the sink. FILTER ONLY - all 6 columns kept
(no projection), so this isolates the cost of the processing hop + filtering vs base-1's
pure wire ceiling. Mirrors one cell of the 20-node design.

## Topology (3 nodes)
```
[source .80:8007] --> [proc .81:8017] --> [sink .85:8027]
   switch A            switch A             switch B
     raw intra-switch (A)      proc->sink crosses A->B uplink
```
- source->proc: intra-switch (raw stays local), like the 20-node design
- proc->sink: cross-switch A->B (1 GbE)

## Data (same as base-1, reused)
- schema: no_extra (6 col) = auction,bidder,price,channel,url,date_time (keep url; channel needed for filter)
- 20M tuples/source. Uses the SAME cache file as base-1 on .80:
  `nexmark_bid_20000000_0_1.parquet`  (already in place)
- iterations: 3

## Fixed
- proc: FILTER only (no projection), keeps 6 col; proc compression = source codec, no dict
- source: compression + dict(channel) per the sweep

## Swept (96 experiments)
| dim | values |
|---|---|
| compression x encoding (4) | none/raw · dict/raw · zstd · zstd+dict  (lz4 dropped: slow) |
| buffering (2) | on, off |
| batch (3) | 25k, 50k, 100k (all maxbps) |
| selectivity (4) | all · channel>'H' (50%) · channel<'B' (12.5%) · channel>'z' (0%) |

96 experiments x 2 iterations = 192 runs.
name pattern: `maxbps_<batch>k_<buffered|unbuffered>_<none|enc|comp|comp_enc>_<all|gtH|ltB|gtz>`
(visualizer: opt-mode = none/enc/comp/comp_enc; query facet = selectivity (all/gtH/ltB/gtz))

## Prerequisites
1. Deploy the 3 nodes:
   `ansible-playbook -i experiment-configurations/base-2/inventory.ini scripts/deployment.yml -K -e "ansible_python_interpreter=/usr/bin/python3"`
2. Data: the 20M no_extra parquet is already on .80 from base-1 (nexmark_bid_20000000_0_1.parquet). No new data prep.

## Launch (duck)
```
python -m woolmilk.run_cluster --config experiment-configurations/base-2/cluster-base-2.json --mode remote
python -m woolmilk.benchmark --config-file experiment-configurations/base-2/benchmark-base-2.json --experiment-dir experiment-configurations/base-2/results --mode remote
```

## Regenerate
`python local-scripts/generate_base2_config.py --out-dir experiment-configurations/base-2`
(local-scripts gitignored; edit SRC/PROC/SINK/COMP_ENC/BATCHES/SELECTIVITY there.)
