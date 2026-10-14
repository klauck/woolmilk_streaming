"""Reading the config directory (benchmark.json + cluster.json) and the
addresses/SSH targets derived from it."""
import json
import sys
from pathlib import Path


def read_json(config_dir, name):
    path = Path(config_dir) / name
    if not path.exists():
        sys.exit(f"error: {path} not found")
    return json.loads(path.read_text())


def remote_servers(bench):
    return bench.get("config", {}).get("remote_servers", {})


def source_endpoints(bench):
    return [node["server_address"] for node in bench.get("source_nodes", [])]


def cluster_endpoints(cluster):
    return (
        [node["server_address"] for node in cluster.get("sink_nodes", [])]
        + [node["server_address"] for node in cluster.get("processing_nodes", [])]
    )


def pi_target(servers, host):
    cfg = servers.get(host)
    if not cfg:
        sys.exit(f"error: host {host} missing from config remote_servers")
    return f"{cfg['username']}@{host}"
