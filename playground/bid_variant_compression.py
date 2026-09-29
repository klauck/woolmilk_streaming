import argparse
import time

import pyarrow as pa
import pyarrow.parquet as pq

CODECS = ["lz4_raw", "zstd"]
FILES = [
    "data/bid-variants/bid_all_3000000.parquet",
    "data/bid-variants/bid_no_extra_5000000.parquet",
    "data/bid-variants/bid_no_extra_url_12000000.parquet",
]


def serialize(batch):
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batch.schema) as writer:
        writer.write_batch(batch)
    return sink.getvalue()


def levels(codec, max_level):
    if not pa.Codec.supports_compression_level(codec):
        return [None]
    low = max(1, pa.Codec.minimum_compression_level(codec))
    high = min(max_level, pa.Codec.maximum_compression_level(codec))
    return list(range(low, high + 1))


def measure(table, codec, level, batch_size):
    codec_obj = pa.Codec(codec, compression_level=level) if level is not None else pa.Codec(codec)
    orig = 0
    comp = 0
    elapsed = 0.0
    for batch in table.to_reader(max_chunksize=batch_size):
        buf = serialize(batch)
        start = time.perf_counter()
        out = codec_obj.compress(buf)
        elapsed += time.perf_counter() - start
        orig += len(buf)
        comp += len(out)
    return orig / 10**6, comp / 10**6, orig / comp, elapsed * 1000


def parse_args():
    ap = argparse.ArgumentParser(description="Bid variant compression ratio (lz4 vs zstd)")
    ap.add_argument("--files", nargs="*", default=FILES)
    ap.add_argument("--batch_size", type=int, default=10_000)
    ap.add_argument("--max_level", type=int, default=5)
    return ap.parse_args()


def main():
    args = parse_args()
    print(f"{'file':<34}{'codec':<10}{'level':>6}{'orig (MB)':>12}{'comp (MB)':>12}{'ratio':>8}{'ms':>10}")
    print("-" * 92)
    for path in args.files:
        table = pq.read_table(path).combine_chunks()
        for codec in CODECS:
            for level in levels(codec, args.max_level):
                orig, comp, ratio, ms = measure(table, codec, level, args.batch_size)
                lvl = "-" if level is None else str(level)
                print(f"{path.split('/')[-1]:<34}{codec:<10}{lvl:>6}{orig:>12.2f}{comp:>12.2f}{ratio:>8.2f}{ms:>10.2f}")


if __name__ == "__main__":
    main()
