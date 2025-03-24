import pyarrow.flight as fl
from datafusion import SessionContext
import argparse

class ExitNodeFlightServer(fl.FlightServerBase):
    """
    A Flight server that receives data from ProcessorNodes
    """
    def __init__(self, host="0.0.0.0", port=8820,  **kwargs):
        location = f"grpc://{host}:{port}"
        super().__init__(location, **kwargs)

        self._host = host
        self._port = port

        self.ctx = SessionContext()
        # total_rows received
        self.total_rows = 0

    def do_put(self, context, descriptor, reader, writer):
        """
        Receives a stream of record batches from a ProcessorNode
        """
        total_rows = 0

        for batch in reader:
            total_rows += batch.data.num_rows

        self.total_rows += total_rows
        
        print(f"Received {total_rows} rows, total so far: {self.total_rows}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Start the ExitNodeFlightServer")
    parser.add_argument("--host", type=str, default="0.0.0.0", help="Server host")
    parser.add_argument("--port", type=int, default=8820, help="Server port")
    args = parser.parse_args()
    
    server = ExitNodeFlightServer(host=args.host, port=args.port)
    print(f"Exit Flight Serving on {args.host}:{args.port}")
    server.serve()
    