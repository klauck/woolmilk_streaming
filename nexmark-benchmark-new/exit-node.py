import argparse
import pyarrow as pa
import pyarrow.flight as fl

class ExitFlightServer(fl.FlightServerBase):
    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 8815,
    ) -> None:
        location = f"grpc://{host}:{port}"
        super().__init__(location)

    def do_put(self, context, descriptor, reader, writer) -> None:
        path = descriptor.path[0].decode() if descriptor.path else "bids"
        
        batches = []
        for batch_no, batch in enumerate(reader, start=1):
            batches.append(batch.data)

        table = pa.Table.from_batches(batches)

        print(f"[Exit] Recieved data for stream '{path}' with {table.num_rows} rows")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="ExitFlightServer"
    )
    parser.add_argument(
        "--host", default="0.0.0.0",
        help="Host to bind on (default: 0.0.0.0)"
    )
    parser.add_argument(
        "--port", type=int, default=8915,
        help="Port to bind on (default: 8915)"
    )
    args = parser.parse_args()

    server = ExitFlightServer(host=args.host, port=args.port)
    print(f"ExitFlightServer listening on grpc://{args.host}:{args.port}")
    server.serve()