import pyarrow.parquet as pq

from woolmilk.data_generator import NexmarkDataGenerator

ROWS = 10_000_000
COLUMNS = ["auction", "bidder", "price", "channel", "date_time"]
OUT = "playground/bids.parquet"


def main():
    gen = NexmarkDataGenerator(
        chunk_size=100_000,
        no_records=ROWS,
        event_type="bid",
        executable="nexmark",
    )
    writer = None
    total = 0
    for _, _, bid_tbl, _ in gen.generate():
        if bid_tbl.num_rows == 0:
            continue
        bid_tbl = bid_tbl.select(COLUMNS)
        if writer is None:
            writer = pq.ParquetWriter(OUT, bid_tbl.schema)
        writer.write_table(bid_tbl)
        total += bid_tbl.num_rows
    if writer is not None:
        writer.close()
    print(f"wrote {total} rows -> {OUT}")


if __name__ == "__main__":
    main()
