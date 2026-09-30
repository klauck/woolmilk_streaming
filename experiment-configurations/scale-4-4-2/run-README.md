# Run: scale-4-4-2 (4 source -> 4 processing -> 2 sink)

Scaling step: two BALANCED halves, each fully intra-switch (topology-aware, no uplink).

## Topology (10 nodes)
```
 switch B:  85 -> 86 ─┐              switch A:  80 -> 81 ─┐
            87 -> 88 ─┴> sink .89               82 -> 83 ─┴> sink .84
 chains: 85->86->89, 87->88->89 (B) ; 80->81->84, 82->83->84 (A)
```
- both halves LOCAL: source->proc and proc->sink stay on the same switch (no A<->B crossing)
- 2 procs -> 1 sink per half (each sink NIC 125 MB/s shared by 2 procs)

## Data
- schema: no_extra (6 col), 10M/source (40M total across 4 sources)
- sources: .85 .87 .80 .82  (each needs the 10M no_extra file; distinct-per-source recommended)

## Fixed
- proc: FILTER only (no projection), keep 6 col; proc compression = source codec
- NO encoding

## Swept (72 experiments)
| dim | values |
|---|---|
| compression (3) | none, lz4, zstd |
| buffering (2) | on, off |
| batch (3) | 10k, 25k, 50k (maxbps) |
| queries / selectivity (4) | all(100%) · gtH(50%) · ltB(12.5%) · gtz(0%) |

72 experiments x 3 iterations = 216 runs.

## Launch (duck)
```
python -m woolmilk.run_cluster --config experiment-configurations/scale-4-4-2/cluster-scale-4-4-2.json --mode remote
python -m woolmilk.benchmark --config-file experiment-configurations/scale-4-4-2/benchmark-scale-4-4-2.json --experiment-dir experiment-configurations/scale-4-4-2/results --mode remote
```

## Regenerate
`python local-scripts/generate_scale442_config.py --out-dir experiment-configurations/scale-4-4-2`
