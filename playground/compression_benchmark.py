import random
import time

import pyarrow as pa

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


def serialize(table):
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue()


def levels(codec):
    if not pa.Codec.supports_compression_level(codec):
        return [None]
    low = max(1, pa.Codec.minimum_compression_level(codec))
    high = pa.Codec.maximum_compression_level(codec)
    return list(range(low, high + 1))


def run():
    buf = serialize(make_data(ROWS))
    orig = len(buf)
    print(f"{'codec':<10}{'level':>7}{'orig':>12}{'comp':>12}{'ratio':>8}{'ms':>10}")
    print("-" * 59)
    for codec in CODECS:
        for lvl in levels(codec):
            codec_obj = pa.Codec(codec, compression_level=lvl) if lvl is not None else pa.Codec(codec)
            try:
                start = time.perf_counter()
                out = codec_obj.compress(buf)
                ms = (time.perf_counter() - start) * 1000
            except pa.ArrowNotImplementedError:
                print(f"{codec:<10}{'-':>7}{'not supported one-shot':>42}")
                break
            level_str = "-" if lvl is None else str(lvl)
            print(f"{codec:<10}{level_str:>7}{orig:>12}{len(out):>12}{orig / len(out):>8.2f}{ms:>10.2f}")


if __name__ == "__main__":
    run()
