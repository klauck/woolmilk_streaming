import argparse
import copy
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from time import sleep
from typing import Dict, List, Optional

import pyarrow as pa
from pyarrow import flight
from run_cluster import (
    Config,
    DeploymentRunner,
    RemoteServerConfig,
    SourceNode,
    parse_config,
)
from util import SourceNodeActions, SourceNodeStatus

SOURCE_NODE_PORT_START = 8210


@dataclass
class ExperimentConfig:
    iterations: int
    source_nodes: List[SourceNode]
    name: Optional[str] = None


def parse_benchmark_config(config_file: Path):
    global SOURCE_NODE_PORT_START

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

            included_source_node.server_address = (
                f"{included_source_node.deployment_server}:{SOURCE_NODE_PORT_START}"
            )
            SOURCE_NODE_PORT_START += 1

            experiment_source_nodes.append(included_source_node)

        experiments.append(
            ExperimentConfig(
                iterations=experiment["iterations"],
                source_nodes=experiment_source_nodes,
                name=experiment.get("name"),
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

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        if experiment.name:
            experiment_base_dir = Path(experiment_dir) / f"{timestamp}_{experiment.name}"
        else:
            experiment_base_dir = Path(experiment_dir) / f"{timestamp}"

        for iteration in range(experiment.iterations):
            print(
                f"            Starting iteration {iteration + 1}/{experiment.iterations}"
            )

            for i, source_node in enumerate(experiment.source_nodes):
                source_node.experiment_id = experiment_id
                source_node.iteration_id = iteration
                source_node.id = i

            current_experiment_dir = experiment_base_dir / f"itr_{iteration}"

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
                include_timestamp=False,
            )

            runner.deploy()
            sleep(2)

            prepare_source_nodes(experiment.source_nodes)
            start_sending(experiment.source_nodes)
            wait_until_complition(experiment.source_nodes)
            collect_cluster_nodes_logs(cluster_nodes, current_experiment_dir)

            runner.cleanup()
            print(
                f"            Completed iteration {iteration + 1}/{experiment.iterations}"
            )


def prepare_source_nodes(nodes: List[SourceNode]):
    print("Preparing source nodes....")

    nodes_to_be_prepared = list(nodes)

    while len(nodes_to_be_prepared) > 0:
        try:
            for node in list(nodes_to_be_prepared):
                if node.server_address is None:
                    continue

                client = flight.FlightClient(f"grpc://{node.server_address}")
                result = list(
                    client.do_action(flight.Action(SourceNodeActions.GET_STATUS, b""))
                )

                if result:
                    status = result[0].body.to_pybytes().decode("utf-8")

                    print(f"[{node.server_address}] Status: {status}")

                    if status == SourceNodeStatus.NOT_STARTED:
                        print(f"[{node.server_address}] Generating data...")
                        client.do_action(
                            flight.Action(SourceNodeActions.GENERATE_DATA, b"")
                        )
                    if status == SourceNodeStatus.DATA_GENERATED:
                        nodes_to_be_prepared.remove(node)

        except Exception as e:
            print(f"Failed to check status: {e}")
            pass

        sleep(2)

    print("All source nodes ready.")


def start_sending(nodes: List[SourceNode]):
    print("Starting to send data from source nodes...")

    for node in nodes:
        if node.server_address is None:
            continue

        client = flight.FlightClient(f"grpc://{node.server_address}")
        client.do_action(flight.Action(SourceNodeActions.SEND_DATA, b""))

    print("Done.")


def wait_until_complition(nodes: List[SourceNode]):
    nodes_to_wait = list(nodes)

    while len(nodes_to_wait) > 0:
        for node in list(nodes_to_wait):
            if node.server_address is None:
                continue

            client = flight.FlightClient(f"grpc://{node.server_address}")

            result = list(
                client.do_action(flight.Action(SourceNodeActions.GET_STATUS, b""))
            )

            if result:
                status = result[0].body.to_pybytes().decode("utf-8")

                print(f"[{node.server_address}] Status: {status}")

                if status == SourceNodeStatus.DONE:
                    nodes_to_wait.remove(node)

        sleep(2)


def collect_cluster_nodes_logs(cluster_nodes, current_experiment_dir):
    # collect log files for specified cluster nodes:
    for node in cluster_nodes:
        client = flight.FlightClient(f"grpc://{node['address']}")
        result = client.do_action("get_logs")
        print(result)
        for data in result:
            log_bytes = data.body.to_pybytes().decode("utf-8")
            file_name = (
                Path(__file__).parent
                / current_experiment_dir
                / (node["type"] + "__" + node["address"].replace(":", "_") + ".json")
            )
            with open(file_name, "w+") as f:
                f.write(log_bytes)
            print(json.loads(log_bytes))
        client.do_action("delete_logs")


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
