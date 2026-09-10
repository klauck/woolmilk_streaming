import argparse
import time

import pyarrow as pa
import pyarrow.parquet as pq


def ipc_size(batch):
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batch.schema) as writer:
        writer.write_batch(batch)
    return len(sink.getvalue())


def encode_column(batch, column):
    arrays = []
    for i, name in enumerate(batch.schema.names):
        col = batch.column(i)
        if name == column:
            col = col.dictionary_encode()
        arrays.append(col)
    return pa.RecordBatch.from_arrays(arrays, names=list(batch.schema.names))


def run(table, batch_sizes, columns):
    print(f"{'column':<14}{'avg. batch size':>14}{'orig (MB)':>12}{'enc (MB)':>12}{'ratio':>8}{'ms':>10}")
    print("-" * 70)
    for column in columns:
        for batch_size in batch_sizes:
            orig = 0
            enc = 0
            elapsed = 0.0
            actual_batch_sizes = []
            for batch in table.to_reader(max_chunksize=batch_size):
                actual_batch_sizes.append(batch.num_rows)
                orig += ipc_size(batch)
                start = time.perf_counter()
                encoded = encode_column(batch, column)
                elapsed += time.perf_counter() - start
                enc += ipc_size(encoded)
            print(f"{column:<14}{sum(actual_batch_sizes)/len(actual_batch_sizes):>14}{orig / 10**6:>12.2f}{enc / 10**6:>12.2f}{orig / enc:>8.2f}{elapsed * 1000:>10.2f}")


def parse_args():
    ap = argparse.ArgumentParser(description="Benchmark pyarrow dictionary encoding per column")
    ap.add_argument("--input-file", dest="input_file", required=True)
    ap.add_argument("--columns", required=True, help="comma-separated columns to encode")
    ap.add_argument("--batch_size", default="100000", help="comma-separated batch sizes")
    return ap.parse_args()


def main():
    args = parse_args()
    table = pq.read_table(args.input_file).combine_chunks()
    columns = [c.strip() for c in args.columns.split(",")]
    batch_sizes = [int(b) for b in args.batch_size.split(",")]
    run(table, batch_sizes, columns)


if __name__ == "__main__":
    main()
