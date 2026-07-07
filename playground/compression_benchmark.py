import argparse
import random
import time

import pyarrow as pa
import pyarrow.parquet as pq

CODECS = ["snappy", "lz4_frame", "lz4_raw", "zstd", "gzip", "brotli", "bz2"]
ROWS = 200_000


def make_data(n):
    rng = random.Random(0)
    first = ["Alice", "Bob", "Carol", "Heidi", "Ivan", "Mallory", "Walter", "Zara"]
    last = ["Smith", "Jones", "Brown", "Wright", "Green", "Hall"]
    cities = ["Berlin", "Hamburg", "Munich", "Cologne"]
    states = ["BE", "HH", "BY", "NW"]
    return pa.table(
        {
            "id": pa.array(range(n), pa.int64()),
            "name": [f"{rng.choice(first)} {rng.choice(last)}" for _ in range(n)],
            "email_address": [f"user{i}@example.com" for i in range(n)],
            "credit_card": [f"{rng.randint(1000, 9999)}-{rng.randint(1000, 9999)}" for _ in range(n)],
            "city": [rng.choice(cities) for _ in range(n)],
            "state": [rng.choice(states) for _ in range(n)],
            "date_time": pa.array([1_700_000_000_000 + i * 10 for i in range(n)], pa.int64()),
            "extra": [f"extra_{rng.randint(0, 1_000_000)}" for _ in range(n)],
        }
    )


def serialize(batch):
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batch.schema) as writer:
        writer.write_batch(batch)
    return sink.getvalue()


def levels(codec, max_level):
    if not pa.Codec.supports_compression_level(codec):
        return [None]
    low = max(1, pa.Codec.minimum_compression_level(codec))
    if max_level:
        high = min(max_level, pa.Codec.maximum_compression_level(codec))
    else:
        high = pa.Codec.maximum_compression_level(codec)
    return list(range(low, high + 1))


def run(table, batch_size, max_level):
    print(f"{'codec':<10}{'level':>7}{'orig (MB)':>12}{'comp (MB)':>12}{'ratio':>8}{'ms':>10}")
    print("-" * 59)
    for codec in CODECS:
        for lvl in levels(codec, max_level):
            codec_obj = pa.Codec(codec, compression_level=lvl) if lvl is not None else pa.Codec(codec)
            orig = 0
            comp = 0
            elapsed = 0.0
            failed = False
            for batch in table.to_reader(max_chunksize=batch_size):
                buf = serialize(batch)
                try:
                    start = time.perf_counter()
                    out = codec_obj.compress(buf)
                    elapsed += time.perf_counter() - start
                except pa.ArrowNotImplementedError:
                    failed = True
                    break
                orig += len(buf)
                comp += len(out)
            if failed:
                print(f"{codec:<10}{'-':>7}{'not supported one-shot':>42}")
                break
            level_str = "-" if lvl is None else str(lvl)
            print(f"{codec:<10}{level_str:>7}{orig/10**6:>12.2f}{comp/10**6:>12.2f}{orig / comp:>8.2f}{elapsed * 1000:>10.2f}")


def parse_args():
    ap = argparse.ArgumentParser(description="Benchmark pyarrow compression codecs and levels")
    ap.add_argument("--input-file", dest="input_file", default=None, help="parquet file to read; random data if omitted")
    ap.add_argument("--batch_size", type=int, default=10_000)
    ap.add_argument("--max_level", type=int, default=None)
    return ap.parse_args()


def main():
    args = parse_args()
    table = pq.read_table(args.input_file) if args.input_file else make_data(ROWS)
    run(table, args.batch_size, args.max_level)


if __name__ == "__main__":
    main()
