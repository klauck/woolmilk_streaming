import argparse
import copy
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from time import sleep
from typing import Dict, List

import pyarrow as pa
import pyarrow.flight
from run_cluster import (
    Config,
    DeploymentRunner,
    RemoteServerConfig,
    SourceNode,
    parse_config,
)


@dataclass
class ExperimentConfig:
    iterations: int
    source_nodes: List[SourceNode]


def parse_benchmark_config(config_file: Path):
    config = parse_config(config_file)

    benchmark_config = json.loads(config_file.read_text())
    source_experiments = benchmark_config.get("source_nodes_experiments", None)
    if not source_experiments:
        print('No experiments specified, expected list "source_nodes_experiments"')
        exit(1)

    cluster_nodes = benchmark_config.get("cluster_nodes", [])

    experiments: List[ExperimentConfig] = []

    for experiment in source_experiments:
        experiment_source_nodes = []

        for i, source_node_offset in enumerate(experiment["included_nodes"]):
            included_source_node = copy.deepcopy(config.source_nodes[source_node_offset])

            if "overridden_params" in experiment:
                assert len(experiment["overridden_params"]) == len(
                    experiment["included_nodes"]
                )
                for key, value in experiment["overridden_params"][i].items():
                    setattr(included_source_node, key, value)

            experiment_source_nodes.append(included_source_node)

        experiments.append(
            ExperimentConfig(
                iterations=experiment["iterations"], source_nodes=experiment_source_nodes
            )
        )

    return {
        "experiments": experiments,
        "remote_servers": config.remote_servers,
        "cluster_nodes": cluster_nodes,
    }


def benchmark(config_path: Path, experiment_dir: str, mode: str):
    combined_config = parse_benchmark_config(config_path)
    experiments: List[ExperimentConfig] = combined_config["experiments"]
    remote_servers: Dict[str, RemoteServerConfig] = combined_config["remote_servers"]
    cluster_nodes = combined_config["cluster_nodes"]

    print("Starting benchmark...")

    for experiment_id, experiment in enumerate(experiments):
        print(f"    Starting experiment {experiment_id + 1}/{len(experiments)}")
        print(f"        Number of source nodes: {len(experiment.source_nodes)}")
        print(f"        Iterations: {experiment.iterations}")

        for iteration in range(experiment.iterations):
            print(
                f"            Starting iteration {iteration + 1}/{experiment.iterations}"
            )

            for i, source_node in enumerate(experiment.source_nodes):
                source_node.experiment_id = experiment_id
                source_node.iteration_id = iteration
                source_node.id = i

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            current_experiment_dir = (
                Path(experiment_dir)
                / f"{timestamp}__experiment_{experiment_id}_iter_{iteration}"
            )

            current_config = Config(
                remote_servers=remote_servers,
                sink_nodes=[],
                processing_nodes=[],
                source_nodes=experiment.source_nodes,
            )

            runner = DeploymentRunner(
                config=current_config,
                log_dir=str(current_experiment_dir) if mode == "local" else "logs",
                mode=mode,
                log_to_file=True,
                local_log_dir=str(current_experiment_dir),
            )

            runner.deploy()
            sleep(5)

            # collect log files for specified cluster nodes:
            for node in cluster_nodes:
                client = pa.flight.FlightClient(f"grpc://{node['address']}")
                result = client.do_action("get_logs")
                print(result)
                for data in result:
                    log_bytes = data.body.to_pybytes().decode("utf-8")
                    file_name = (
                        Path(__file__).parent
                        / current_experiment_dir
                        / timestamp
                        / (
                            node["type"]
                            + "__"
                            + node["address"].replace(":", "_")
                            + ".json"
                        )
                    )
                    with open(file_name, "w+") as f:
                        f.write(log_bytes)
                    print(json.loads(log_bytes))
                client.do_action("delete_logs")

            runner.cleanup()
            print(
                f"            Completed iteration {iteration + 1}/{experiment.iterations}"
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run benchmarks for Woolmilk.")
    parser.add_argument(
        "--config-file",
        type=str,
        required=True,
        help="Path to the benchmark configuration file.",
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

    benchmark(config_path, experiment_dir=args.experiment_dir, mode=args.mode)
