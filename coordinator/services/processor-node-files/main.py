
import argparse
import uvicorn
from api import (app)

def main():
    parser = argparse.ArgumentParser(description="ProcessorNode API")
    parser.add_argument("--host",    type=str, default="0.0.0.0",
                        help="FastAPI bind host")
    parser.add_argument("--port",    type=int, default=8915,
                        help="FastAPI listen port")
    parser.add_argument("--node-id", type=str, default="processor_node_1",
                        help="Identifier for this processor node")
    parser.add_argument("--exit-host", type=str, default=None, 
                        help="Optional exit node host")
    parser.add_argument("--exit-port", type=int, default=None,
                        help="Optional exit node port")
    args = parser.parse_args()

    print("Starting ProcessorNode API with the following parameters:")
    print(f"Host: {args.host}")
    print(f"Port: {args.port}")
    print(f"Node ID: {args.node_id}")
    print(f"Exit Host: {args.exit_host}")
    print(f"Exit Port: {args.exit_port}")

    # Initialize shared state
    app.state.node_id = args.node_id
    app.state.exit_host = args.exit_host
    app.state.exit_port = args.exit_port
    app.state.current_query = None

    uvicorn.run(app, host=args.host, port=args.port, reload=False)

if __name__ == "__main__":
    main()
