import argparse
import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
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
    store_input: Optional[str] = None


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
                store_input=sn.get("store_input"),
            )
        )

    return Config(
        sink_nodes=sink_nodes,
        processing_nodes=processing_nodes,
        source_nodes=source_nodes,
        servers=servers,
    )


class DeploymentRunner:
    def __init__(self, config: Config, log_dir: str, mode: str = "local", log_to_file: bool = False):
        self.config = config
        self.log_dir = log_dir
        self.mode = mode
        self.log_to_file = log_to_file
        self.src_dir = Path(__file__).resolve().parent
        self.processes = []

        os.makedirs(log_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.log_prefix = f"{timestamp}_"

    def get_server_config(self, host: str) -> Optional[ServerConfig]:
        """Get server config for host"""
        return self.config.servers.get(host)

    def _spawn_process(self, role: str, identifier: str, cmd: List[str], host: Optional[str] = None) -> None:

        if self.mode == "remote" and host:
            server_config = self.get_server_config(host)
            if not server_config:
                raise RuntimeError(f"No server config found for host {host}")

            if self.log_to_file:
                remote_log = f"{server_config.base_dir}/logs/{log_file.name}"
                remote_cmd = (
                        f"mkdir -p {server_config.base_dir}/logs && "
                        f"cd {server_config.base_dir} && "
                        f"source {server_config.python_env}/bin/activate && "
                        + " ".join(cmd)
                        + f" > {remote_log} 2>&1"
                )
                print(f"[{role}] Remote {host}, logs -> {remote_log}")
            else:
                remote_cmd = (
                        f"cd {server_config.base_dir} && "
                        f"source {server_config.python_env}/bin/activate && "
                        + " ".join(cmd)
                )
                print(f"[{role}] Remote {host}, streaming logs to terminal")

            ssh_cmd = ["ssh", f"{server_config.username}@{host}", remote_cmd]
            proc = subprocess.Popen(ssh_cmd)

        else:  # local
            if self.log_to_file:
                log_handle = open(log_file, "w")
                print(f"[{role}] Local logs -> {log_file}")
                proc = subprocess.Popen(cmd, stdout=log_handle, stderr=subprocess.STDOUT, text=True)
            else:
                print(f"[{role}] Local streaming logs to terminal")
                proc = subprocess.Popen(cmd, text=True)

        self.processes.append((role, identifier, proc))

    def run_sink_nodes(self):
        print("Starting sink nodes...")
        for sink in self.config.sink_nodes:
            host, port = sink.server_address.split(":")
            cmd = [
                sys.executable,
                "-u",
                str(self.src_dir / "sink_node.py"),
                "--port",
                str(port),
            ]
            if sink.result_folder:
                cmd.append("--result-folder")
                cmd.append(str(self.src_dir / sink.result_folder))
            print(f"Running locally: {' '.join(cmd)}")

            self._spawn_process("sink", sink.server_address, cmd, host=host)

    def run_processing_nodes(self):
        print("Starting processing nodes...")
        for proc_node in self.config.processing_nodes:
            host, port = proc_node.server_address.split(":")
            cmd = [
                sys.executable,
                "-u",
                str(self.src_dir / "processing_node.py"),
                "--port",
                str(port),
                "--forward-node",
                proc_node.forward_node,
                "--query-result-schema",
                json.dumps(proc_node.query_result_schema),
            ]
            if proc_node.query:
                cmd.append("--query")
                cmd.append(proc_node.query)

            self._spawn_process("processing", proc_node.server_address, cmd, host=host)
        time.sleep(1)

    def run_source_nodes(self):
        print("Starting source nodes...")
        for source_node in self.config.source_nodes:
            processing_nodes = ",".join(source_node.processing_nodes)

            host = source_node.deployment_server

            cmd = [
                sys.executable,
                "-u",
                str(self.src_dir / "source_node.py"),
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
            if source_node.store_input:
                cmd.append("--store-input")
                cmd.append(str(self.src_dir / Path(source_node.store_input)))

            self._spawn_process("source", source_node.stream, cmd, host=host)

    def cleanup(self):
        print("\nCleaning up processes...")
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
    parser.add_argument("--log-dir", default="logs", help="Base log directory path")
    parser.add_argument("--mode", choices=["local", "remote"], default="local", help="Deployment mode")
    parser.add_argument("--log-to-file", action="store_true", help="Redirect logs to files instead of terminal")
    args = parser.parse_args()

    if not Path(args.config).exists():
        print(f"Config file {args.config} not found!")
        sys.exit(1)

    config = parse_config(args.config)
    runner = DeploymentRunner(config, args.log_dir, mode=args.mode, log_to_file=args.log_to_file)
    runner.deploy()

    time.sleep(2)
    print("Press any key to exit")
    input()
    runner.cleanup()
