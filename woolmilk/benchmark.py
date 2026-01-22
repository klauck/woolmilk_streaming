import argparse
import copy
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from time import sleep
from typing import Dict, List
import pyarrow.flight as flight

from run_cluster import (
    Config,
    DeploymentRunner,
    RemoteServerConfig,
    SourceNode,
    parse_config,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent

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

    return {"experiments": experiments, "remote_servers": config.remote_servers}


def benchmark(config_path: Path, experiment_dir: str, mode: str, use_flight_logs: bool = False, experiment_name: str = None):
    combined_config = parse_benchmark_config(config_path)
    experiments: List[ExperimentConfig] = combined_config["experiments"]
    remote_servers: Dict[str, RemoteServerConfig] = combined_config["remote_servers"]
    
    # Load cluster nodes config if available
    cluster_nodes = []
    with open(config_path, 'r') as f:
        data = json.load(f)
        cluster_nodes = data.get("cluster_nodes", [])

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
            if experiment_name:
                base_exp_dir = PROJECT_ROOT / "woolmilk" / "experiments" / experiment_name
                base_exp_dir.mkdir(parents=True, exist_ok=True)
                
                current_experiment_dir = (
                    base_exp_dir
                    / f"{timestamp}__experiment_{experiment_id}_iter_{iteration}"
                )
            else:
                current_experiment_dir = (
                    PROJECT_ROOT
                    / experiment_dir
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
                log_dir=str(current_experiment_dir / "source") if use_flight_logs else str(current_experiment_dir),
                mode=mode,
                log_to_file=True,
            )

            runner.deploy()
            sleep(5)
            runner.cleanup()
            
            if use_flight_logs:
                print("    Fetching logs from cluster nodes...")
                
                (current_experiment_dir / "processing").mkdir(parents=True, exist_ok=True)
                (current_experiment_dir / "sink").mkdir(parents=True, exist_ok=True)
                
                for node in cluster_nodes:
                    node_type = node["type"]
                    address = node["address"]
                    try:
                        client = flight.FlightClient(f"grpc://{address}")
                        result = client.do_action(flight.Action("get_and_clear_logs", b""))
                        logs_json_str = next(result).body.to_pybytes().decode("utf-8")
                        logs_list = json.loads(logs_json_str)
                        
                        log_file = current_experiment_dir / node_type / f"{node_type}_{address.replace(':', '_')}.log"
                        with open(log_file, "w") as f:
                            for log_entry in logs_list:
                                f.write(f"WM_LOG= {json.dumps(log_entry)}\n")
                        print(f"      fetched {node_type} logs from {address}")
                    except Exception as e:
                        print(f"      Warning: Failed to fetch logs from {address}: {e}")

            print(
                f"            Completed iteration {iteration + 1}/{experiment.iterations}"
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run benchmarks for Woolmilk.")
    parser.add_argument(
        "--config-file", type=str, required=True, help="Path to the benchmark configuration file."
    )
    parser.add_argument(
        "--experiment-dir",
        type=str,
        help="Directory to store experiment logs and results.",
        default="woolmilk/experiments",
    )
    parser.add_argument(
        "--mode",
        choices=["local", "remote"],
        default="local",
        help="Execution mode: local or remote.",
    )
    parser.add_argument(
        "--experiment-name",
        type=str,
        help="Name of the experiment (groups logs under experiments/{name})",
        default=None,
    )
    parser.add_argument(
        "--flight-logs",
        action="store_true",
        help="Enable fetching logs via Flight actions (requires cluster_nodes in config)",
    )
    args = parser.parse_args()

    config_path = Path(args.config_file)

    benchmark(
        config_path, 
        experiment_dir=args.experiment_dir, 
        mode=args.mode,
        use_flight_logs=args.flight_logs,
        experiment_name=args.experiment_name
    )
