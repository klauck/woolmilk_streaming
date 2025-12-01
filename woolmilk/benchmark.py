import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from time import sleep
from typing import Dict, List

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


@dataclass
class BenchmarkConfig:
    experiments: List[ExperimentConfig]


def parse_benchmark_config(config_file: Path):
    config = parse_config(config_file)

    benchmark_config = json.loads(config_file.read_text())
    source_nodes_exp = benchmark_config.get("source_nodes_exp", None)

    benchmarks: List[BenchmarkConfig] = []

    if source_nodes_exp:
        for exp in source_nodes_exp:
            experiments: List[ExperimentConfig] = []

            include_nodes = exp["include_nodes"]
            iterations = exp["iterations"]

            selected_source_nodes = []

            for idx, source_node in enumerate(config.source_nodes):
                if (idx + 1) in include_nodes:
                    selected_source_nodes.append(source_node)

            for override in exp["overridden_params"]:
                # create a copy of the source node
                local_source_nodes = [
                    SourceNode(**node.__dict__) for node in selected_source_nodes
                ]

                for i, node in enumerate(local_source_nodes):
                    for key, value in override.items():
                        setattr(node, key, value)

                experiments.append(
                    ExperimentConfig(
                        iterations=iterations, source_nodes=local_source_nodes
                    )
                )

            benchmarks.append(BenchmarkConfig(experiments=experiments))

    return {"benchmarks": benchmarks, "remote_servers": config.remote_servers}


def benchmark(config_path: Path, experiment_dir: str, mode: str):
    combined_config = parse_benchmark_config(config_path)
    benchmarks: List[BenchmarkConfig] = combined_config["benchmarks"]
    remote_servers: Dict[str, RemoteServerConfig] = combined_config["remote_servers"]

    print("Starting benchmark...")

    for benchmark_id, benchmark_config in enumerate(benchmarks):
        print(f"    Starting bechmark {benchmark_id + 1}/{len(benchmarks)}")
        print(f"    Number of experiments: {len(benchmark_config.experiments)}")

        for experiment_id, experiment in enumerate(benchmark_config.experiments):
            print(
                f"        Starting experiment {experiment_id + 1}/{len(benchmark_config.experiments)}"
            )
            print(f"        Number of source nodes: {len(experiment.source_nodes)}")
            print(f"        Iterations: {experiment.iterations}")

            for iteration in range(experiment.iterations):
                print(
                    f"            Starting iteration {iteration + 1}/{experiment.iterations}"
                )
                current_experiment_dir = (
                    Path(experiment_dir)
                    / f"benchmark_{benchmark_id + 1}_exp_{experiment_id + 1}_iter_{iteration + 1}"
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
                runner.cleanup()
                print(
                    f"            Completed iteration {iteration + 1}/{experiment.iterations}"
                )


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

    benchmark(config_path, experiment_dir=args.experiment_dir, mode=args.mode)
