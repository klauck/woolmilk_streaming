# Run: scale-2-2-1 (2 source -> 2 processing -> 1 sink)

Scaling step: two parallel source->proc chains fan into one sink. All switch B (intra-switch).

## Topology (5 nodes, all switch B)
```
  source .85 --> proc .86 ─┐
                           ├──> sink .89
  source .87 --> proc .88 ─┘
  chain 1: 85 -> 86 -> 89 ;  chain 2: 87 -> 88 -> 89
```

## Data
- schema: no_extra (6 col), 10M tuples/source (20M total across 2 sources)
- .87 has its file (from hop-2). .85 needs the 10M no_extra file copied in.
- distinct-per-source recommended (2 unique chunks).

## Fixed
- proc: FILTER only (no projection), keeps 6 col; proc compression = source codec
- NO encoding

## Swept (72 experiments)
| dim | values |
|---|---|
| compression (3) | none, lz4, zstd |
| buffering (2) | on, off |
| batch (3) | 10k, 25k, 50k (maxbps) |
| queries / selectivity (4) | all(100%) · channel>'H'(50%) · channel<'B'(12.5%) · channel>'z'(0%) |

72 experiments x 3 iterations = 216 runs.
name: `maxbps_<raw|lz4|zstd>_<batch>k_<buffered|unbuffered>_<none|comp>_<all|gtH|ltB|gtz>`

## Launch (duck)
```
python -m woolmilk.run_cluster --config experiment-configurations/scale-2-2-1/cluster-scale-2-2-1.json --mode remote
python -m woolmilk.benchmark --config-file experiment-configurations/scale-2-2-1/benchmark-scale-2-2-1.json --experiment-dir experiment-configurations/scale-2-2-1/results --mode remote
```

## Regenerate
`python local-scripts/generate_scale221_config.py --out-dir experiment-configurations/scale-2-2-1`
