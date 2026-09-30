# Run: scale-4-4-1 (4 source -> 4 processing -> 1 sink)

Scaling step: 4 parallel source->proc chains fan into one sink. Spans switch A + B.

## Topology (9 nodes)
```
 switch B:  85 -> 86 ─┐
            87 -> 88 ─┤
 switch A:  80 -> 81 ─┼──> sink .89 (switch B)
            82 -> 83 ─┘
 chains: 85->86->89, 87->88->89, 80->81->89, 82->83->89
```
- source->proc: intra-switch (85->86, 87->88 on B; 80->81, 82->83 on A)
- proc->sink:  86->89, 88->89 LOCAL (B) ; 81->89, 83->89 CROSS A->B uplink
- 4 procs -> 1 sink NIC (125 MB/s) = heavy fan-in bottleneck

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
python -m woolmilk.run_cluster --config experiment-configurations/scale-4-4-1/cluster-scale-4-4-1.json --mode remote
python -m woolmilk.benchmark --config-file experiment-configurations/scale-4-4-1/benchmark-scale-4-4-1.json --experiment-dir experiment-configurations/scale-4-4-1/results --mode remote
```

## Regenerate
`python local-scripts/generate_scale441_config.py --out-dir experiment-configurations/scale-4-4-1`
