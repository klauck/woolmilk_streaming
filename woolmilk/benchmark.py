import argparse
import json
from pathlib import Path
from time import sleep
from typing import List

from run_cluster import Config, DeploymentRunner, RemoteServerConfig, SourceNode


def parse_source_nodes_experiments(config_file: str) -> dict:
    source_nodes: List[SourceNode] = []

    with open(config_file, "r") as f:
        config_data = json.load(f)

    servers = {}
    if "config" in config_data and "remote_servers" in config_data["config"]:
        for host, server_data in config_data["config"]["remote_servers"].items():
            servers[host] = RemoteServerConfig(**server_data)

    source_nodes_data = config_data.get("source_node_experiments", None)

    assert isinstance(
        source_nodes_data, dict
    ), "source_node_experiments must be a dictionary."

    stream = source_nodes_data.get("stream")
    overall_tuples = source_nodes_data.get("overall_tuples")
    tuples_per_batch = source_nodes_data.get("tuples_per_batch")
    processing_nodes_list = source_nodes_data.get("processing_nodes", [])
    thread_count = source_nodes_data.get("thread_count", 1)
    deployment_server = source_nodes_data.get("deployment_server", None)
    experiments = source_nodes_data.get("experiments", [])

    for experiment in experiments:
        node_stream = experiment.get("stream", stream)
        node_overall_tuples = experiment.get("overall_tuples", overall_tuples)
        node_tuples_per_batch = experiment.get("tuples_per_batch", tuples_per_batch)
        node_processing_nodes = experiment.get("processing_nodes", processing_nodes_list)
        node_thread_count = experiment.get("thread_count", thread_count)
        node_deployment_server = experiment.get("deployment_server", deployment_server)

        source_nodes.append(
            SourceNode(
                processing_nodes=node_processing_nodes,
                stream=node_stream,
                overall_tuples=node_overall_tuples,
                tuples_per_batch=node_tuples_per_batch,
                thread_count=node_thread_count,
                deployment_server=node_deployment_server,
            )
        )

    return {"servers": servers, "experiments": source_nodes}


def benchmark(config_file: str, experiment_dir: str, mode: str):
    configuration = parse_source_nodes_experiments(config_file)
    exprs = configuration["experiments"]
    remote_servers = configuration["servers"]

    print(f"Parsed {len(exprs)} experiments from configuration in {mode} mode.")

    for expr in exprs:
        print("=" * 40)
        print("Running experiment with configuration:")
        print(f"Processing Nodes: {expr.processing_nodes}")
        print(f"Stream: {expr.stream}")
        print(f"Overall Tuples: {expr.overall_tuples}")
        print(f"Tuples per Batch: {expr.tuples_per_batch}")
        print(f"Thread Count: {expr.thread_count}")
        print(f"Deployment Server: {expr.deployment_server}")
        print("=" * 40)

        config: Config = Config(
            source_nodes=[expr],
            remote_servers=remote_servers,
            processing_nodes=[],
            sink_nodes=[],
        )

        # on remote mode, logs are stored in a common logs/ directory,
        # and then copied to local experiment_dir
        log_dir = "logs" if mode == "remote" else experiment_dir

        runner = DeploymentRunner(
            config,
            mode=mode,
            log_dir=log_dir,
            log_to_file=True,
            local_log_dir=experiment_dir,
        )
        runner.deploy()
        sleep(5)
        runner.cleanup()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run benchmarks for Woolmilk.")
    parser.add_argument(
        "--config-file", type=str, help="Path to the benchmark configuration file."
    )
    parser.add_argument(
        "--experiment-dir",
        type=str,
        help="Directory to store experiment logs and results.",
        default="experiments",
    )
    parser.add_argument(
        "--mode",
        choices=["local", "remote"],
        default="local",
        help="Execution mode: local or remote.",
    )
    args = parser.parse_args()

    config_path = Path(args.config_file)

    assert config_path.exists(), f"Config file {args.config_file} does not exist."

    benchmark(str(config_path), experiment_dir=args.experiment_dir, mode=args.mode)
