import argparse
import json
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
    generator_executable: Optional[str] = None


@dataclass
class Config:
    sink_nodes: List[SinkNode]
    processing_nodes: List[ProcessingNode]
    source_nodes: List[SourceNode]
    servers: Dict[str, ServerConfig] = field(default_factory=dict)


def parse_config(json_path: Path) -> Config:
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
                generator_executable=sn.get("generator_executable"),
            )
        )

    return Config(
        sink_nodes=sink_nodes,
        processing_nodes=processing_nodes,
        source_nodes=source_nodes,
        servers=servers,
    )


class DeploymentRunner:
    def __init__(
        self, config: Config, log_dir: str, mode: str = "local", log_to_file: bool = False
    ):
        self.config = config
        self.mode = mode
        self.processes = []

        self.log_to_file = log_to_file
        if log_to_file:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            self.log_dir = Path(log_dir) / timestamp

    def get_base_dir(self, host):
        if self.mode == "local":
            return Path(__file__).resolve().parent
        else:
            assert self.mode == "remote"
            return Path(self.get_server_config(host).base_dir)

    def get_server_config(self, host: str) -> Optional[ServerConfig]:
        """Get server config for host"""
        return self.config.servers.get(host)

    def _spawn_process(
        self,
        node_type: str,
        node_identifier: str,
        cmd: List[str],
        base_dir: Path,
        host: Optional[str] = None,
    ) -> None:
        if self.log_to_file:
            self.log_dir = base_dir / self.log_dir
            log_file = (
                self.log_dir / f"{node_type}_{node_identifier.replace(':', '_')}.log"
            )
            if self.mode == "local":
                self.log_dir.mkdir(parents=True, exist_ok=True)

        if self.mode == "remote" and host:
            server_config = self.get_server_config(host)
            if not server_config:
                raise RuntimeError(f"No server config found for host {host}")

            if self.log_to_file:
                remote_cmd = (
                    f"mkdir -p {self.log_dir} && "
                    f"cd {str(base_dir)} && "
                    f"source {server_config.python_env}/bin/activate && "
                    + " ".join(cmd)
                    + f" > {log_file} 2>&1"
                )
                print(f"[{node_type}] Remote {host}, logs -> {log_file}")
            else:
                remote_cmd = (
                    f"cd {str(base_dir)} && "
                    f"source {server_config.python_env}/bin/activate && " + " ".join(cmd)
                )
                print(f"[{node_type}] Remote {host}, streaming logs to terminal")

            ssh_cmd = ["ssh", f"{server_config.username}@{host}", remote_cmd]
            proc = subprocess.Popen(ssh_cmd)

        else:  # local
            if self.log_to_file:
                with open(log_file, "w") as log_handle:
                    print(f"[{node_type}] Local logs -> {log_file}")
                    proc = subprocess.Popen(
                        cmd, stdout=log_handle, stderr=subprocess.STDOUT, text=True
                    )
            else:
                print(f"[{node_type}] Local streaming logs to terminal")
                proc = subprocess.Popen(cmd, text=True)

        self.processes.append((node_type, node_identifier, proc))

    def run_sink_nodes(self):
        print("Starting sink nodes...")
        for sink in self.config.sink_nodes:
            host, port = sink.server_address.split(":")
            base_dir = self.get_base_dir(host)
            cmd = [
                "python",
                "-u",
                str(base_dir / "sink_node.py"),
                "--port",
                str(port),
            ]
            if sink.result_folder:
                cmd.append("--result-folder")
                cmd.append(str(base_dir / sink.result_folder))

            self._spawn_process(
                "sink", sink.server_address, cmd, base_dir=base_dir, host=host
            )

    def run_processing_nodes(self):
        print("Starting processing nodes...")
        for proc_node in self.config.processing_nodes:
            host, port = proc_node.server_address.split(":")
            base_dir = self.get_base_dir(host)
            cmd = [
                "python",
                "-u",
                str(base_dir / "processing_node.py"),
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

            self._spawn_process(
                "processing", proc_node.server_address, cmd, base_dir=base_dir, host=host
            )
        time.sleep(1)

    def run_source_nodes(self):
        print("Starting source nodes...")
        for source_node in self.config.source_nodes:
            processing_nodes = ",".join(source_node.processing_nodes)

            host = source_node.deployment_server
            base_dir = self.get_base_dir(host)

            cmd = [
                "python",
                "-u",
                str(base_dir / "source_node.py"),
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
                cmd.append(str(base_dir / Path(source_node.store_input)))

            if source_node.generator_executable:
                cmd.append("--generator-executable")
                cmd.append(source_node.generator_executable)

            self._spawn_process(
                "source", source_node.stream, cmd, base_dir=base_dir, host=host
            )

    def cleanup(self):
        print("\nCleaning up processes...")
        for node_type, node_identifier, proc in self.processes:
            print(f"    Terminate process ({node_type}, {node_identifier}, {proc})")
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
    parser.add_argument(
        "--mode", choices=["local", "remote"], default="local", help="Deployment mode"
    )
    parser.add_argument(
        "--log-to-file",
        action="store_true",
        help="Redirect logs to files instead of terminal",
    )
    args = parser.parse_args()

    config_path = Path(args.config)
    if not config_path.exists():
        print(f"Config file {args.config} not found!")
        sys.exit(1)

    config = parse_config(args.config)
    runner = DeploymentRunner(
        config, args.log_dir, mode=args.mode, log_to_file=args.log_to_file
    )
    runner.deploy()

    time.sleep(2)
    print("Press any key to exit")
    input()
    runner.cleanup()
