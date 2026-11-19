import tempfile
from pathlib import Path

# import datafusion.functions.spark as spark # Ab DataFusion 54
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
from datafusion import SessionContext, col

ctx = SessionContext()
table = pa.table({"channel": ["Apple", "Google", "Apple"], "price": [10, 20, 30]})
batch = pa.record_batch({"auction": [1, 2, 3, 4, 5, 6, 7, 8]})


### partition_nth_indices
print("partition_nth_indices")
values = pa.array([7, 2, 9, 4, 1, 8, 3, 6])
new_values = values.take(pc.partition_nth_indices(values, pivot=3))
print(new_values) # [1, 2, 3, 4, 6, 7, 8, 9]

### pyarrow dataset partitioning
print("\nds.partitioning")
out = Path(tempfile.mkdtemp()) / "out"
part = ds.partitioning(pa.schema([("channel", pa.string())]), flavor="hive")
ds.write_dataset(table, out, format="parquet", partitioning=part)
for path in out.rglob("*.parquet"):
    print(path.relative_to(out.parent))
    # out\channel=Apple\part-0.parquet
    # out\channel=Google\part-0.parquet

### grouped aggregation functions 
print("\ne.g. hash_sum")
print(table.group_by("channel").aggregate([("price", "hash_sum")]))

### datafusion repartition_by_hash
print("\nrepartition_by_hash")
df = ctx.from_arrow(batch).repartition_by_hash(col("auction"), num=16)
print(len(df.collect_partitioned())) 
# 1 anstatt 16, wahrscheinlich weil es wegoptimiert wird
# print(df.optimized_logical_plan().display_indent())
# print(df.execution_plan().display_indent())

### Eingebaute und performante Hash-Funtkion, Ab DataFusion 54
# print("\nxxhash64")
# h = spark.xxhash64(col("auction"))
# ctx.from_arrow(batch).select(col("auction"), h).show()