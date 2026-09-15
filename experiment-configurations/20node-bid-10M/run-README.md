# Run: 20node-bid-10M

20-node fan-in scale-out, bid, reduced matrix. Purpose: show whether the
compression / encoding winner changes when the system is **network-bound**
(vs the CPU-bound 3-node case).

## Topology (fan-in)
```
s1,s2   -> p1 -> k1
s3,s4   -> p2 -> k1
s5,s6   -> p3 -> k1
s7,s8   -> p4 -> k2
s9,s10  -> p5 -> k2
s11,s12 -> p6 -> k2
```
12 source (.80-.91) -> 6 processing (.92-.97) -> 2 sink (.98/.99). One node per Pi.

## Data
- Each source sends **10M UNIQUE** bid tuples → **120M total, no duplicates** across 12
  sources (saturates the network — exposes the "compression pays off at scale" effect).
- 5-col curated bid `auction,bidder,price,channel,date_time`, `channel` encoded.
- Uniqueness trick (no code change): every source looks for the SAME filename
  `nexmark_bid_10000000_0_1.parquet`, but each source Pi holds a DIFFERENT 10M chunk
  (chunk i = nexmark offset i*10M). Same name, different content per Pi.
- Generate the 12 chunks: `python scripts/generate_bid_chunks.py` → `data/chunks/chunk_00..11.parquet`.
- IMPORTANT: all 12 must be pre-placed. If a source cache-MISSES it generates an offset-0
  chunk = duplicate. Never miss → always pre-copy.

## Matrix (18 experiments)
fixed: batch 50k, buffering on, maxbps, iterations 1.
| dim | values |
|---|---|
| compression | none, lz4, zstd |
| encoding | none, dictionary(channel) |
| selectivity | all(100%), channel>'H'(50%), channel<'B'(12.5%) |
3 x 2 x 3 = 18. Bump `iterations` to 3 in `scripts/generate-config.py` for the real run.

## Prerequisites
1. All 20 Pis (.80-.99) up + deployed:
   `ansible-playbook -i experiment-configurations/20node-bid-10M/inventory.ini scripts/deployment.yml -K -e "ansible_python_interpreter=/usr/bin/python3"`
2. Copy each UNIQUE chunk to its source Pi (chunk i → Pi .80+i), renamed to the cache name:
   ```
   for i in $(seq 0 11); do
     ip=$((80 + i)); c=$(printf '%02d' $i)
     ssh picocluster@192.168.2.$ip "mkdir -p ~/usama/woolmilk_streaming/woolmilk/input_data"
     scp data/chunks/chunk_$c.parquet \
       picocluster@192.168.2.$ip:~/usama/woolmilk_streaming/woolmilk/input_data/nexmark_bid_10000000_0_1.parquet
   done
   ```

## Launch (duck)
```
# A: cluster (6 processing + 2 sink)
python -m woolmilk.run_cluster --config experiment-configurations/20node-bid-10M/cluster-20node-bid-10M.json --mode remote
# B: benchmark (12 sources + push config)
python -m woolmilk.benchmark --config-file experiment-configurations/20node-bid-10M/benchmark-20node-bid-10M.json --experiment-dir experiment-configurations/20node-bid-10M --mode remote
```

## Analysis (the figure)
Compare per treatment, 3-node vs 20-node:
- bytes on wire (input_bytes vs output_bytes), throughput (MBps/Gbps), per-stage cost
  (querying/encoding/sending) from the collected `processing__*.json`.
- expected: compression's benefit grows at 20 (network-bound); encoding steady;
  zstd+dict combo best for transport.
