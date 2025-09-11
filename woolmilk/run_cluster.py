import argparse
import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional


@dataclass
class ServerConfig:
    username: str
    password: str
    base_dir: str
    python_env: str


@dataclass
class SinkNode:
    server_address: str
    result_folder: Optional[str] = None


@dataclass
class ProcessingNode:
    server_address: str
    forward_node: str
    query_result_schema: Dict
    query: Optional[str] = None


@dataclass
class SourceNode:
    processing_nodes: List[str]
    stream: str
    overall_tuples: int
    tuples_per_batch: int
    thread_count: int = 1
    deployment_server: Optional[str] = None


@dataclass
class Config:
    sink_nodes: List[SinkNode]
    processing_nodes: List[ProcessingNode]
    source_nodes: List[SourceNode]
    servers: Dict[str, ServerConfig] = field(default_factory=dict)


def parse_config(json_path: str) -> Config:
    with open(json_path, "r") as f:
        data = json.load(f)

    # Parse servers config
    servers = {}
    if "config" in data and "servers" in data["config"]:
        for host, server_data in data["config"]["servers"].items():
            servers[host] = ServerConfig(**server_data)

    sink_nodes = [SinkNode(**sn) for sn in data.get("sink_nodes", [])]
    processing_nodes = [ProcessingNode(**pn) for pn in data.get("processing_nodes", [])]

    source_nodes = []
    for sn in data.get("source_nodes", []):
        source_nodes.append(
            SourceNode(
                processing_nodes=sn.get("processing_nodes"),
                stream=sn.get("stream"),
                overall_tuples=sn.get("overall_tuples"),
                tuples_per_batch=sn.get("tuples_per_batch"),
                thread_count=sn.get("thread_count", 1),
                deployment_server=sn.get("deployment_server"),
            )
        )

    return Config(
        sink_nodes=sink_nodes,
        processing_nodes=processing_nodes,
        source_nodes=source_nodes,
        servers=servers,
    )


class DeploymentRunner:
    def __init__(self, config: Config, log_dir):
        self.config = config
        self.log_dir = log_dir
        self.mode = "local"
        self.src_dir = os.path.dirname(os.path.abspath(__file__))
        self.processes = []

        os.makedirs(log_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.log_prefix = f"{timestamp}_"

    def get_server_config(self, host: str) -> Optional[ServerConfig]:
        """Get server config for host"""
        return self.config.servers.get(host)

    def run_sink_nodes(self):
        """Start all sink nodes"""
        print("Starting sink nodes...")
        for sink in self.config.sink_nodes:
            log_file = os.path.join(
                self.log_dir,
                f"{self.log_prefix}sink_{sink.server_address.replace(':', '_')}.log",
            )

            host, port = sink.server_address.split(":")
            server_config = self.get_server_config(host)

            if self.mode == "local" or server_config is None:
                # Local execution
                cmd = [
                    sys.executable,
                    "-u",
                    os.path.join(self.src_dir, "sink_node.py"),
                    "--port",
                    str(port),
                ]
                if sink.result_folder:
                    cmd.append("--result-folder")
                    cmd.append(f"{self.src_dir}/{sink.result_folder}")
                print(f"Running locally: {' '.join(cmd)}")

                process = subprocess.Popen(
                    cmd,
                    text=True,
                    bufsize=1,
                )
                self.processes.append(("sink", sink.server_address, process))

    def run_processing_nodes(self):
        """Start all processing nodes"""
        print("Starting processing nodes...")
        for proc_node in self.config.processing_nodes:
            log_file = os.path.join(
                self.log_dir,
                f"{self.log_prefix}processing_"
                f"{proc_node.server_address.replace(':', '_')}.log",
            )

            host, port = proc_node.server_address.split(":")
            server_config = self.get_server_config(host)

            if self.mode == "local" or server_config is None:
                # Local execution
                cmd = [
                    sys.executable,
                    "-u",
                    os.path.join(self.src_dir, "processing_node.py"),
                    "--port",
                    str(port),
                    "--forward-node",
                    proc_node.forward_node,
                    "--query",
                    proc_node.query,
                    "--query-result-schema",
                    json.dumps(proc_node.query_result_schema),
                ]
                print(f"Running locally: {' '.join(cmd)}")

                proc = subprocess.Popen(
                    cmd,
                    text=True,
                    bufsize=1,
                )
                self.processes.append(("processing", proc_node.server_address, proc))
        time.sleep(1)

    def run_source_nodes(self):
        """Start all source nodes"""
        print("Starting source nodes...")
        for i, source_node in enumerate(self.config.source_nodes):
            processing_nodes = ",".join(source_node.processing_nodes)

            host = source_node.deployment_server
            server_config = self.get_server_config(host)

            if self.mode == "local" or server_config is None:
                # Local execution
                cmd = [
                    sys.executable,
                    "-u",
                    os.path.join(self.src_dir, "source_node.py"),
                    "--stream",
                    source_node.stream,
                    "--tuples-per-batch",
                    str(source_node.tuples_per_batch),
                    "--overall-tuples",
                    str(source_node.overall_tuples),
                    "--processing-nodes",
                    processing_nodes,
                    "--thread-count",
                    str(source_node.thread_count),
                ]

                print(f"Running locally: {' '.join(cmd)}")

                proc = subprocess.Popen(
                    cmd,
                    text=True,
                    bufsize=1,
                )
                self.processes.append(("source", source_node.stream, proc))

    def cleanup(self):
        """Terminate all running processes"""
        for proc_type, identifier, proc in self.processes:
            print(f"    Terminate process ({proc_type}, {identifier}, {proc})")
            try:
                if hasattr(proc, "terminate"):
                    proc.terminate()
                    if hasattr(proc, "wait"):
                        proc.wait(timeout=5)
            except Exception as e:
                print(f"    Warning: terminate failed for {proc} ({e})")
                try:
                    if hasattr(proc, "kill"):
                        proc.kill()
                except Exception as kill_err:
                    print(f"    Error: could not kill {proc} ({kill_err})")

    def deploy(self):
        """Deploy the entire system"""
        print(f"\n{'='*60}")
        print(f"STARTING WOOLMILK STREAMING DEPLOYMENT ({self.mode.upper()} MODE)")
        print(f"{'='*60}")

        try:
            self.run_sink_nodes()
            self.run_processing_nodes()
            self.run_source_nodes()
        except Exception as e:
            print(f"Error during deployment: {e}")
            self.cleanup()
            sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="WoolMilk Streaming Deployment")
    parser.add_argument("--config", default="config.json", help="Configuration file path")
    parser.add_argument("--log-dir", default="logs", help="Log directory path")
    args = parser.parse_args()

    if not os.path.exists(args.config):
        print(f"Config file {args.config} not found!")
        sys.exit(1)

    config = parse_config(args.config)
    runner = DeploymentRunner(config, args.log_dir)
    runner.deploy()
    time.sleep(2)
    print("Press any key to exit")
    input()
    runner.cleanup()
