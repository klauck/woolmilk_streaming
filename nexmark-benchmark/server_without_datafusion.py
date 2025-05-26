import os
import argparse
import pyarrow.parquet as pq
import pyarrow.flight as fl


def parse_parquet_files(files_str: str) -> dict:
    """Convert "key=value,key=value" into a dict."""
    mapping = {}
    for pair in files_str.split(","):
        if not pair:
            continue
        key, val = pair.split("=")
        mapping[key.strip()] = val.strip()
    return mapping


class NodeFlightServer(fl.FlightServerBase):
    def __init__(
        self,
        bids_path: str,
        *,
        host: str = "0.0.0.0",
        port: int = 8815,
        read_type: str = "memory",
    ) -> None:
        location = f"grpc://{host}:{port}"
        super().__init__(location)

        if read_type not in {"memory", "disk"}:
            raise ValueError("read_type must be 'memory' or 'disk'")

        self._host = host
        self._port = port
        self._read_type = read_type
        self._bids_path = bids_path
        self._bids_table = None
        self._is_sent = False

        if read_type == "memory":
            # Load once and keep in memory.
            self._bids_table = pq.read_table(bids_path, read_dictionary=False).replace_schema_metadata(None)

    # ---------------------------------------------------------------------
    # Flight overrides
    # ---------------------------------------------------------------------

    def do_get(self, context, ticket):  # noqa: D401, N802 (Flight naming)
        """Ignore *ticket* and return the entire *bids* table in one shot."""
        if self._is_sent:
            # return empty table
            return fl.RecordBatchStream(fl.RecordBatchReader([]))
        
        print(f"NodeFlightServer: do_get() called with ticket: {ticket}")
        self._is_sent = True

        if self._read_type == "memory":
            table = self._bids_table
        else:
            table = pq.read_table(self._bids_path, read_dictionary=False).replace_schema_metadata(None)

        return fl.RecordBatchStream(table.to_reader())


# -------------------------------------------------------------------------
# Command‑line entrypoint
# -------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Simple Node Flight Server (full‑table streaming)")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind to (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=8815, help="Port to bind to (default: 8815)")

    parser.add_argument(
        "--read_type",
        choices=["memory", "disk"],
        default="memory",
        help="Read Parquet into memory or stream from disk (default: memory)",
    )

    parser.add_argument(
        "--data_dir",
        default=".",
        help="Directory containing Parquet files (default: current directory)",
    )

    parser.add_argument(
        "--parquet_files",
        default="bids=bids.parquet",
        help="Comma‑separated key=value list mapping dataset names to Parquet files. Only 'bids' is used.",
    )

    args = parser.parse_args()

    parquet_map = parse_parquet_files(args.parquet_files)
    if "bids" not in parquet_map:
        parser.error("--parquet_files must include a 'bids' dataset entry, e.g. 'bids=bids.parquet'")

    # Resolve relative path against data_dir.
    bids_path = os.path.join(args.data_dir, parquet_map["bids"])

    print(f"Serving *bids* Parquet dataset from: {bids_path}")
    print(f"Flight endpoint: grpc://{args.host}:{args.port}")

    server = NodeFlightServer(
        bids_path=bids_path,
        host=args.host,
        port=args.port,
        read_type=args.read_type,
    )

    server.serve()


if __name__ == "__main__":
    main()
