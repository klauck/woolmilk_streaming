
import argparse
import uvicorn
from api import (app)

def main():
    parser = argparse.ArgumentParser(description="ProcessorNode API")
    parser.add_argument("--host",    type=str, required=True,
                        help="FastAPI bind host")
    parser.add_argument("--port",    type=int, required=True,
                        help="FastAPI listen port")
    parser.add_argument("--node-id", type=str, required=True,
                        help="Identifier for this processor node")
    parser.add_argument("--exit-host", type=str, default=None,
                        help="Optional exit node host")
    parser.add_argument("--exit-port", type=int, default=None,
                        help="Optional exit node port")
    args = parser.parse_args()

    # Initialize shared state
    app.state.node_id = args.node_id
    app.state.exit_host = args.exit_host
    app.state.exit_port = args.exit_port
    app.state.current_query = None

    uvicorn.run(app, host=args.host, port=args.port, reload=False)

if __name__ == "__main__":
    main()
