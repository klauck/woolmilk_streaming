# Run: 20node-perhop (per-hop treatment + projection)

20-node fan-in. Purpose: show that dictionary **encoding is column-local** — it pays only
where the low-cardinality column (`channel`) is on the wire. Once processing **projects
channel out**, the proc→sink hop needs only **compression**; encoding there is a no-op.

Replaces the uniform sweep (`20node-bid-10M`) as the active 20-node design.

## Topology
12 source (.80-.91) -> 6 processing (.92-.97) -> 2 sink (.98/.99), fan-in (see 20node-bid-10M/topology.png; shape identical).

## Data
Same as 20node-bid-10M: each source a UNIQUE 10M chunk (`nexmark_bid_10000000_0_1.parquet`),
120M total. Generate with `scripts/generate_bid_chunks.py`, copy chunk i -> source Pi .80+i.

## Experiments (4) — per-hop treatment
| name | source->proc (hop1) | proc query | proc->sink (hop2) |
|---|---|---|---|
| perhop_raw_keep        | none              | SELECT * (keep channel)      | none |
| perhop_comp_keep       | zstd              | SELECT * (keep channel)      | zstd |
| perhop_enccomp_keep    | zstd + dict(chan) | SELECT * (keep channel)      | zstd + dict(chan) |
| perhop_enccomp_proj_comp | zstd + dict(chan) | SELECT auction,bidder,price,date_time (DROP channel) | zstd only |

Fixed: batch 50k, buffering on, maxbps, iterations 1. node_config carries BOTH source
and processing addresses (source-side treatment is applied here, unlike the uniform matrix).

## The argument (from the numbers)
- comp_keep vs enccomp_keep → encoding's benefit while channel is on the wire.
- enccomp_keep vs enccomp_proj_comp → projecting channel removes it entirely (biggest
  proc->sink drop); afterwards only compression is needed, encoding has nothing to encode.

Validated locally (100k/source): sink output of the proj experiment = 4 cols
(`auction,bidder,price,date_time`, NO channel); proc->sink IPC dropped 53.9->38.4 MB;
0 GENERATING_DATA; all 4 experiments exit 0.

## Prerequisites
1. 20 Pis deployed:
   `ansible-playbook -i experiment-configurations/20node-perhop/inventory.ini scripts/deployment.yml -K -e "ansible_python_interpreter=/usr/bin/python3"`
2. Copy unique chunks (chunk i -> Pi .80+i):
   `for i in $(seq 0 11); do ip=$((80+i)); c=$(printf '%02d' $i); ssh picocluster@192.168.2.$ip "mkdir -p ~/usama/woolmilk_streaming/woolmilk/input_data"; scp data/chunks/chunk_$c.parquet picocluster@192.168.2.$ip:~/usama/woolmilk_streaming/woolmilk/input_data/nexmark_bid_10000000_0_1.parquet; done`

## Launch (duck)
```
python -m woolmilk.run_cluster --config experiment-configurations/20node-perhop/cluster-20node-perhop.json --mode remote
python -m woolmilk.benchmark --config-file experiment-configurations/20node-perhop/benchmark-20node-perhop.json --experiment-dir experiment-configurations/20node-perhop --mode remote
```

## Regenerate config
`python scripts/generate_perhop_config.py --out-dir experiment-configurations/20node-perhop`
