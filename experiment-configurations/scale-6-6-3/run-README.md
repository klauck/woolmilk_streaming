# Run: scale-6-6-3 (6 source -> 6 processing -> 3 sink)

Scaling step: three BALANCED thirds, each fully intra-switch (topology-aware, no uplink).
NOTE: user's "87->86" read as 87->88 (1:1 source/proc), same convention as 6/6/1.

## Topology (15 nodes)
```
 switch B:  85->86 ─┐        switch A:  80->81 ─┐        switch C:  90->91 ─┐
            87->88 ─┴> .89              82->83 ─┴> .84              92->93 ─┴> .94
 chains: 85->86->89,87->88->89 (B) ; 80->81->84,82->83->84 (A) ; 90->91->94,92->93->94 (C)
```
- every chain LOCAL to its switch (no A/B/C crossing); each sink fed by 2 procs

## Data
- schema: no_extra (6 col), 10M/source (60M total across 6 sources)
- sources: .85 .87 .80 .82 .90 .92  (each needs the 10M no_extra file; distinct-per-source recommended)

## Fixed
- proc: FILTER only (no projection), keep 6 col; proc compression = source codec ; NO encoding

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
python -m woolmilk.run_cluster --config experiment-configurations/scale-6-6-3/cluster-scale-6-6-3.json --mode remote
python -m woolmilk.benchmark --config-file experiment-configurations/scale-6-6-3/benchmark-scale-6-6-3.json --experiment-dir experiment-configurations/scale-6-6-3/results --mode remote
```

## Regenerate
`python local-scripts/generate_scale663_config.py --out-dir experiment-configurations/scale-6-6-3`
