# Run: scale-6-6-1 (6 source -> 6 processing -> 1 sink)

Scaling step: 6 parallel source->proc chains fan into ONE sink. Spans switch A + B + C.
NOTE: user's chain "87->86" read as 87->88 (1:1 source/proc). Confirm if 2 sources should hit .86.

## Topology (13 nodes)
```
 switch B:  85 -> 86 ─┐
            87 -> 88 ─┤
 switch A:  80 -> 81 ─┼──> sink .89 (switch B)
            82 -> 83 ─┤
 switch C:  90 -> 91 ─┤
            92 -> 93 ─┘
 chains: 85->86, 87->88, 80->81, 82->83, 90->91, 92->93  (all -> 89)
```
- source->proc: intra-switch on each switch
- proc->sink:  86->89, 88->89 LOCAL (B) ; 81/83->89 CROSS A->B ; 91/93->89 CROSS C->B
- 6 procs -> 1 sink NIC (125 MB/s) = 6:1 fan-in -> the sink is the hard bottleneck

## Data
- schema: no_extra (6 col), 10M/source (60M total across 6 sources)
- sources: .85 .87 .80 .82 .90 .92  (each needs the 10M no_extra file; distinct-per-source recommended)

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
python -m woolmilk.run_cluster --config experiment-configurations/scale-6-6-1/cluster-scale-6-6-1.json --mode remote
python -m woolmilk.benchmark --config-file experiment-configurations/scale-6-6-1/benchmark-scale-6-6-1.json --experiment-dir experiment-configurations/scale-6-6-1/results --mode remote
```

## Regenerate
`python local-scripts/generate_scale661_config.py --out-dir experiment-configurations/scale-6-6-1`
