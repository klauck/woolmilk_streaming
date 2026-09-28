# Run: base-1 (source -> sink, pure wire ceiling)

First baseline. NO processing: source streams straight to the sink (Flight do_put).
Measures the raw wire ceiling and the effect of compression + dict-encoding, with no
query/filter/projection in the way. The sink decodes (dictionary_decode_batch) and
records received bytes + MBps/Gbps.

## Topology (2 nodes, cross-switch A -> B)
```
[source .80:8007]  --Flight (compress + dict)-->  [sink .85:8027]
   switch A                 A->B uplink                switch B
```
Cross-switch on purpose: the single source->sink stream crosses ONE 1 GbE uplink
(125 MB/s). Measures the wire ceiling over a real inter-switch link.
- source.processing_nodes = [sink]  (targets the sink directly)
- cluster: sink only, processing_nodes = []

## Data (consistent per-source volume)
- schema: no_extra (6 col) = auction,bidder,price,channel,url,date_time  (drop extra, KEEP url)
- 20,000,000 tuples/source (~2.2 GB raw) -> ~18s at line rate uncompressed, longer than
  the compressed cases so every run reaches steady state.
- iterations: 3

## Swept (36 experiments)
| dim | values |
|---|---|
| compression x encoding (6) | none/raw · dict/raw · lz4 · lz4+dict · zstd · zstd+dict |
| buffering (2) | on, off |
| batch (3) | 25k, 50k, 100k  (all maxbps / unthrottled) |

36 experiments x 3 iterations = 108 runs. No selectivity (sink cannot filter).
name pattern: `maxbps_<batch>k_<buffered|unbuffered>_<none|enc|comp|comp_enc>_<raw|lz4|zstd>`
(visualizer: opt-mode = none/enc/comp/comp_enc, codec shown as the query facet raw/lz4/zstd)

## Prerequisites (do BEFORE launching)
1. Deploy the 2 nodes:
   `ansible-playbook -i experiment-configurations/base-1/inventory.ini scripts/deployment.yml -K -e "ansible_python_interpreter=/usr/bin/python3"`
2. 20M no_extra (6-col) bid parquet on the source Pi (.80), named to the source cache:
   generate `nexmark_bid_20000000_0_1.parquet` (6 col, keep url) and copy to
   `~/usama/woolmilk_streaming/woolmilk/input_data/` on .80.
   (generator: local-scripts/generate_bid_variants.py style with COLUMNS = no_extra, ROWS = 20M)

## Launch (duck)
```
python -m woolmilk.run_cluster --config experiment-configurations/base-1/cluster-base-1.json --mode remote
python -m woolmilk.benchmark --config-file experiment-configurations/base-1/benchmark-base-1.json --experiment-dir experiment-configurations/base-1/results --mode remote
```

## Regenerate
`python local-scripts/generate_base1_config.py --out-dir experiment-configurations/base-1`
(local-scripts is gitignored; edit SRC/SINK/COMP_ENC/BATCHES there.)
