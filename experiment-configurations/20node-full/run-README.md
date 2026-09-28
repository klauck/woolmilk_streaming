# Run: 20node-full (full per-hop sweep)

20-node fan-in, full sweep. Per-hop treatment with projection, over batch-rate x buffering x
selectivity. Shows: encoding+compression pay while `channel` is on the wire (source hop);
after projecting channel out, proc->sink needs only compression.

## Topology
12 source (.80-.91) -> 6 processing (.92-.97) -> 2 sink (.98/.99), fan-in. See topology.png.

## Fixed (every experiment)
- per-hop:     source = lz4 + dict(channel)  ·  proc = lz4 only (channel projected out)
- projection:  DROP channel -> output 4-col: auction,bidder,price,date_time
- compression: lz4
- data:        120M unique bid (10M/source), iterations 3

## Swept (72 experiments)
| dim | values |
|---|---|
| batch-rate pair (9) | (25k,20) (25k,40) (25k,max) · (50k,10) (50k,20) (50k,max) · (100k,5) (100k,10) (100k,max) |
| buffering (2) | on, off |
| selectivity (4) | all, channel>'H', channel<'B', channel>'z' |

throttles: (25k,20)=500k/s (25k,40)=1M/s · (50k,10)=500k/s (50k,20)=1M/s · (100k,5)=500k/s (100k,10)=1M/s · max=unthrottled

## Queries (drop-channel)
```
all  SELECT auction,bidder,price,date_time FROM nexmark_data
gtH  … WHERE channel > 'H'    ltB  … WHERE channel < 'B'    gtz  … WHERE channel > 'z'
```

## Totals
72 experiments x 3 iterations = 216 runs (~4-5h; throttled runs slower).
name: `<bps|maxbps>_<batch>k_<buffered|unbuffered>_comp_enc_<all|gtH|ltB|gtz>` (comp_enc token = visualizer opt-mode pivot)

## Prerequisites
1. 20 Pis deployed:
   `ansible-playbook -i experiment-configurations/20node-full/inventory.ini scripts/deployment.yml -K -e "ansible_python_interpreter=/usr/bin/python3"`
2. Unique chunks on the 12 source Pis (chunk i -> Pi .80+i), renamed to the cache name:
   `for i in $(seq 0 11); do ip=$((80+i)); c=$(printf '%02d' $i); ssh picocluster@192.168.2.$ip "mkdir -p ~/usama/woolmilk_streaming/woolmilk/input_data"; scp data/20node-perhop/chunk_$c.parquet picocluster@192.168.2.$ip:~/usama/woolmilk_streaming/woolmilk/input_data/nexmark_bid_10000000_0_1.parquet; done`

## Launch (duck)
```
python -m woolmilk.run_cluster --config experiment-configurations/20node-full/cluster-20node-full.json --mode remote
python -m woolmilk.benchmark --config-file experiment-configurations/20node-full/benchmark-20node-full.json --experiment-dir experiment-configurations/20node-full/results --mode remote
```

## Regenerate
`python local-scripts/generate_perhop_config.py --out-dir experiment-configurations/20node-full`
(local-scripts is gitignored; edit PAIRS / BUFFERING / SELECTIVITY there.)
