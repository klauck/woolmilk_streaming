import argparse
import json
import shlex
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional


@dataclass
class RemoteServerConfig:
    username: str
    base_dir: str
    python_env: str
    ssh_host: Optional[str] = None
    ssh_port: Optional[int] = None


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
    step: int = -1
    deployment_server: Optional[str] = "127.0.0.1"
    store_input: Optional[bool] = False
    input_folder: Optional[str] = None
    generator_executable: Optional[str] = None
    experiment_id: Optional[int] = None
    iteration_id: Optional[int] = None
    id: Optional[int] = None


@dataclass
class Config:
    sink_nodes: List[SinkNode]
    processing_nodes: List[ProcessingNode]
    source_nodes: List[SourceNode]
    remote_servers: Dict[str, RemoteServerConfig] = field(default_factory=dict)


def parse_config(json_path: Path) -> Config:
    with open(json_path, "r") as f:
        data = json.load(f)

    # Parse servers config
    servers = {}
    if "config" in data and "remote_servers" in data["config"]:
        for host, server_data in data["config"]["remote_servers"].items():
            servers[host] = RemoteServerConfig(**server_data)

    sink_nodes = [SinkNode(**sn) for sn in data.get("sink_nodes", [])]
    processing_nodes = [ProcessingNode(**pn) for pn in data.get("processing_nodes", [])]

    source_nodes = []
    for sn in data.get("source_nodes", []):
        source_nodes.append(SourceNode(**sn))

    return Config(
        sink_nodes=sink_nodes,
        processing_nodes=processing_nodes,
        source_nodes=source_nodes,
        remote_servers=servers,
    )


class DeploymentRunner:
    def __init__(
        self,
        config: Config,
        log_dir: str,
        mode: str = "local",
        log_to_file: bool = False,
        local_log_dir: Optional[str] = None,
        local_results_dir: Optional[str] = None,
        include_timestamp: bool = True,
    ):
        self.config = config
        self.mode = mode
        self.processes = []

        self.log_to_file = log_to_file
        self.local_log_dir = Path(local_log_dir) if local_log_dir else None
        self.local_results_dir = Path(local_results_dir) if local_results_dir else None
        self.include_timestamp = include_timestamp

        if log_to_file:
            self.log_dir = Path(log_dir)

            if include_timestamp:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                self.log_dir = self.log_dir / timestamp

    def build_scp_command(
        self,
        config: RemoteServerConfig,
        remote_path: str,
        local_path: str,
        legacy_host: str,
    ) -> List[str]:
        scp_cmd = ["scp", "-r"]

        if config.ssh_port:
            scp_cmd.extend(["-P", str(config.ssh_port)])

        ssh_host = config.ssh_host if config.ssh_host else legacy_host
        remote_full_path = f"{config.username}@{ssh_host}:{remote_path}"
        scp_cmd.extend([remote_full_path, local_path])

        return scp_cmd

    def copy_remote_results(self):
        assert (
            self.mode == "remote" and self.local_results_dir is not None
        ), "can only copy results in remote mode with local_results_dir specified"

        print("\nCopying results from sink nodes...")
        self.local_results_dir.mkdir(parents=True, exist_ok=True)

        for sink in self.config.sink_nodes:
            host, _ = sink.server_address.split(":")
            try:
                server_config = self.get_remote_server_config(host)

                assert (
                    sink.result_folder is not None
                ), "sink result_folder must be specified to copy results"

                remote_result_folder = self.get_base_dir(host) / Path(sink.result_folder)

                scp_cmd = self.build_scp_command(
                    server_config,
                    str(remote_result_folder),
                    str(self.local_results_dir),
                    host,
                )

                print(f"    Copying results from {host}...")
                result = subprocess.run(scp_cmd, capture_output=True, text=True)
                if result.returncode == 0:
                    print("      Copied results successfully")
                else:
                    print(f"    Error copying from {host}: {result.stderr}")
            except Exception as e:
                print(f"    Warning: failed to copy results from {host}: {e}")

    def get_python(self, host: Optional[str]) -> str:
        if self.mode == "local":
            return "python"
        else:
            assert self.mode == "remote" and host is not None
            return self.get_remote_server_config(host).python_env + "/bin/python"

    def get_base_dir(self, host: Optional[str]) -> Path:
        if self.mode == "local":
            return Path(__file__).resolve().parent
        else:
            assert self.mode == "remote" and host is not None
            return Path(self.get_remote_server_config(host).base_dir)

    def get_remote_server_config(self, host: str) -> RemoteServerConfig:
        """Get server config for host"""
        return self.config.remote_servers[host]

    def quote_if_remote(self, cmd_str: str) -> str:
        if self.mode == "remote":
            return shlex.quote(cmd_str)
        else:
            return cmd_str

    def _spawn_process(
        self,
        node_type: str,
        node_identifier: str,
        cmd: List[str],
        base_dir: Path,
        host: Optional[str] = None,
    ) -> None:
        log_file = None
        log_dir: Optional[Path] = None
        if self.log_to_file:
            log_dir = base_dir / self.log_dir
            log_file = log_dir / f"{node_type}__{node_identifier.replace(':', '_')}.log"

        if self.mode == "remote" and host:
            server_config = self.get_remote_server_config(host)
            if not server_config:
                raise RuntimeError(f"No server config found for host {host}")

            if self.log_to_file and log_file is not None:
                remote_cmd = (
                    f"mkdir -p {log_dir} && "
                    f"cd {str(base_dir)} && " + " ".join(cmd) + f" > {log_file} 2>&1"
                )
                print(f"[{node_type}] Remote {host}, logs -> {log_file}")
            else:
                remote_cmd = f"cd {str(base_dir)} && " + " ".join(cmd)
                print(remote_cmd)
                print(f"[{node_type}] Remote {host}, streaming logs to terminal")

            ssh_cmd = self.get_ssh_connection_command(server_config, host)

            ssh_cmd.extend([remote_cmd])
            proc = subprocess.Popen(ssh_cmd)

        else:  # local
            if self.log_to_file and log_file is not None and log_dir is not None:
                log_dir.mkdir(parents=True, exist_ok=True)
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
                self.get_python(host),
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
                self.get_python(host),
                "-u",
                str(base_dir / "processing_node.py"),
                "--port",
                str(port),
                "--forward-node",
                proc_node.forward_node,
                "--query-result-schema",
                self.quote_if_remote(json.dumps(proc_node.query_result_schema)),
            ]
            if proc_node.query:
                cmd.append("--query")
                cmd.append(self.quote_if_remote(proc_node.query))

            self._spawn_process(
                "processing", proc_node.server_address, cmd, base_dir=base_dir, host=host
            )
        time.sleep(1)

    def run_source_nodes(self):
        print("Starting source nodes...")
        for i, source_node in enumerate(self.config.source_nodes):
            processing_nodes = ",".join(source_node.processing_nodes)

            host = source_node.deployment_server

            base_dir = self.get_base_dir(host)

            cmd = [
                self.get_python(host),
                "-u",
                str(base_dir / "source_node.py"),
                "--stream",
                source_node.stream,
                "--tuples-per-batch",
                str(source_node.tuples_per_batch),
                "--step",
                str(source_node.step),
                "--overall-tuples",
                str(source_node.overall_tuples),
                "--processing-nodes",
                processing_nodes,
            ]
            if source_node.store_input:
                cmd.append("-store-input")
            if source_node.input_folder:
                cmd.append("--input-folder")
                cmd.append(str(base_dir / Path(source_node.input_folder)))

            if source_node.generator_executable:
                cmd.append("--generator-executable")
                cmd.append(source_node.generator_executable)

            if source_node.experiment_id is not None:
                cmd.append("--experiment-id")
                cmd.append(str(source_node.experiment_id))

            if source_node.iteration_id is not None:
                cmd.append("--iteration-id")
                cmd.append(str(source_node.iteration_id))

            if source_node.id is not None:
                cmd.append("--source-node-id")
                cmd.append(str(source_node.id))

            self._spawn_process(
                "source", f"{host}_{i}", cmd, base_dir=base_dir, host=host
            )

    def get_ssh_connection_command(
        self, config: RemoteServerConfig, legacy_host: str
    ) -> List[str]:
        ssh_cmd = ["ssh"]
        if config.ssh_port:
            ssh_cmd.extend(["-p", str(config.ssh_port)])
        ssh_host = config.ssh_host if config.ssh_host else legacy_host
        ssh_cmd.append(f"{config.username}@{ssh_host}")
        return ssh_cmd

    def copy_remote_logs(self):
        assert (
            self.mode == "remote" and self.log_to_file and self.local_log_dir is not None
        ), (
            "can only copy logs in remote mode with log_to_file enabled "
            "and local_log_dir specified"
        )

        print("\nCopying logs from remote servers...")
        self.local_log_dir.mkdir(parents=True, exist_ok=True)

        hosts = set(self.config.remote_servers.keys())

        for host in hosts:
            try:
                server_config = self.get_remote_server_config(host)
                remote_log_dir = self.get_base_dir(host) / self.log_dir
                if not self.include_timestamp:
                    # copy files only but not entire folder
                    remote_log_dir /= "*.log"

                scp_cmd = self.build_scp_command(
                    server_config, str(remote_log_dir), str(self.local_log_dir), host
                )

                print(f"    Copying logs from {host}...")
                result = subprocess.run(scp_cmd, capture_output=True, text=True)
                if result.returncode == 0:
                    print("      Copied logs successfully")
                else:
                    print(f"    Error copying from {host}: {result.stderr}")
            except Exception as e:
                print(f"    Warning: failed to copy logs from {host}: {e}")

        print(f"    Logs copied to: {self.local_log_dir}")

    def cleanup(self):
        print("\nCleaning up processes...")
        for node_type, node_identifier, proc in self.processes:
            print(f"    Terminate process ({node_type}, {node_identifier}, {proc})")
            if self.mode == "remote":
                if node_type in ["sink", "processing"]:
                    host, port = node_identifier.split(":")
                    server_config = self.get_remote_server_config(host)
                    ssh_cmd = self.get_ssh_connection_command(server_config, host)
                    ssh_cmd.extend([f"fuser -k {port}/tcp"])
                    print(ssh_cmd)
                    subprocess.run(ssh_cmd, check=True)
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

        if self.mode == "remote":
            if self.log_to_file and self.local_log_dir is not None:
                self.copy_remote_logs()

            if self.local_results_dir is not None:
                self.copy_remote_results()

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
    parser.add_argument(
        "--local-log-dir",
        help="Local log dir where logs are stored when in remote mode",
    )
    parser.add_argument(
        "--local-results-dir",
        help="Local directory to store copied logs from remote servers",
    )

    args = parser.parse_args()

    if args.log_to_file and args.mode == "remote" and not args.local_log_dir:
        print(
            "Error: --local-log-dir must be specified "
            "when using --log-to-file in remote mode"
        )
        sys.exit(1)

    config_path = Path(args.config)
    if not config_path.exists():
        print(f"Config file {args.config} not found!")
        sys.exit(1)

    config = parse_config(args.config)
    runner = DeploymentRunner(
        config,
        args.log_dir,
        mode=args.mode,
        log_to_file=args.log_to_file,
        local_log_dir=args.local_log_dir if args.local_log_dir else None,
        local_results_dir=args.local_results_dir if args.local_results_dir else None,
    )
    runner.deploy()

    time.sleep(2)
    print("Press any key to exit")
    input()
    runner.cleanup()
