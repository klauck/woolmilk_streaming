import pyarrow.flight as fl
from datafusion import SessionContext

class ExitNodeFlightServer(fl.FlightServerBase):
    def __init__(self, host="0.0.0.0", port=8820,  **kwargs):
        location = f"grpc://{host}:{port}"
        super().__init__(location, **kwargs)

        self._host = host
        self._port = port

        self.ctx = SessionContext()

    def do_put(self, context, descriptor, reader, writer):
        total_rows = 0

        for batch in reader:
            total_rows += batch.data.num_rows
        
        print(f"Received {total_rows} rows.")
    

if __name__ == "__main__":
    host = "0.0.0.0"
    port = 8820
    server = ExitNodeFlightServer()
    print(f"Exit Flight Serving on {host}:{port}")
    server.serve()