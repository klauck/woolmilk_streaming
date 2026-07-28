import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from woolmilk.data_generator import NexmarkDataGenerator

ROWS = 20_000_000
COLUMNS = ["auction", "bidder", "price", "channel", "date_time"]
START_TIMESTAMP = 1785180000000
INCREMENT = 1
OUT = f"nexmark_bid_{ROWS}_0_1.parquet"


def main():
    gen = NexmarkDataGenerator(
        chunk_size=10_000,
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
        n = bid_tbl.num_rows
        date_time = START_TIMESTAMP + (total + np.arange(n, dtype=np.int64)) * INCREMENT
        idx = bid_tbl.schema.get_field_index("date_time")
        bid_tbl = bid_tbl.set_column(idx, "date_time", pa.array(date_time))
        if writer is None:
            writer = pq.ParquetWriter(OUT, bid_tbl.schema)
        writer.write_table(bid_tbl)
        total += n
    if writer is not None:
        writer.close()
    print(f"wrote {total} rows -> {OUT}")


if __name__ == "__main__":
    main()
