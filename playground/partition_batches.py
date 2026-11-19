import time

import datafusion.functions.spark as spark # Ab DataFusion 54
import pyarrow.compute as pc
from datafusion import SessionConfig, SessionContext, col, lit

import woolmilk.source_node

N = 16
ROWS = 1_000_000
BATCH_SIZE = 10_000

ctx = SessionContext(SessionConfig().with_target_partitions(1)) # with_target_partitions ist wichtig
h = spark.xxhash64(col("auction"))
target = ((h % lit(N)) + lit(N)) % lit(N) # hash könnte negativ sein, das target sollte nicht negativ sein, also verschieben zu 0..N-1

def partition(batch):
    targets = ctx.from_arrow(batch).select(target).to_arrow_table().column(0).combine_chunks()
    return {t: batch.filter(pc.equal(targets, t)) for t in range(N)} # type: ignore

# Schnellere Variante: einmal nach Ziel sortieren statt für jedes Ziel zu filtern
# def partition(batch):
#     table = ctx.from_arrow(batch).select(target).to_arrow_table()
#     targets = table.column(0).combine_chunks()
#     order = pc.sort_indices(targets)
#     runs = pc.run_end_encode(targets.take(order))  # type: ignore
#     ordered = batch.take(order)
#     parts, start = {}, 0
#     for t, end in zip(runs.values.to_pylist(), runs.run_ends.to_pylist()):
#         parts[t], start = ordered.slice(start, end - start), end
#     return parts


table = woolmilk.source_node.generate_table(ROWS, "nexmark_bid").combine_chunks()
batches = table.to_batches(max_chunksize=BATCH_SIZE)

start = time.perf_counter()
for batch in batches:
    parts = partition(batch)
seconds = time.perf_counter() - start

print({t: parts[t].num_rows for t in sorted(parts)}) # type: ignore
print(f"{table.num_rows} rows, {table.nbytes / 1e6:.0f} MB, {len(batches)} batches")
print(f"{seconds * 1000:.0f} ms, {table.nbytes / seconds / 1e9:.2f} GB/s")