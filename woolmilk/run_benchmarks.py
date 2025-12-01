import argparse
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from time import sleep

def run_benchmarks(config_folder: Path, mode: str, benchmark_name: str):
    if not config_folder.exists():
        print(f"Error: Config folder {config_folder} does not exist!")
        sys.exit(1)
    
    config_json = config_folder / "config.json"
    benchmark_config_json = config_folder / "benchmark-config.json"
    
    if not config_json.exists():
        print(f"Error: config.json not found in {config_folder}")
        sys.exit(1)
    
    if not benchmark_config_json.exists():
        print(f"Error: benchmark-config.json not found in {config_folder}")
        sys.exit(1)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    benchmark_root = Path(__file__).resolve().parent / "benchmarks"
    benchmark_dir = benchmark_root / f"{benchmark_name}_{mode}_{timestamp}"
    experiments_dir = benchmark_dir / "experiments"
    logs_dir = benchmark_dir / "logs"
    
    benchmark_dir.mkdir(parents=True, exist_ok=True)
    experiments_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"\n{'='*60}")
    print(f"STARTING BENCHMARK RUN")
    print(f"{'='*60}")
    print(f"Benchmark Name: {benchmark_name}")
    print(f"Mode: {mode}")
    print(f"Config Folder: {config_folder}")
    print(f"Output Directory: {benchmark_dir}")
    print(f"{'='*60}\n")
    
    python_exec = sys.executable
    base_dir = Path(__file__).resolve().parent
    run_cluster_script = base_dir / "run_cluster.py"
    benchmark_script = base_dir / "benchmark.py"
    
    run_cluster_log = logs_dir / "run_cluster.log"
    benchmark_log = logs_dir / "benchmark.log"
    
    print("Step 1: Starting cluster nodes...")
    run_cluster_cmd = [
        python_exec,
        "-u",
        str(run_cluster_script),
        "--config", str(config_json),
        "--log-dir", "logs" if mode == "remote" else str(logs_dir / "cluster_logs"),
        "--mode", mode,
        "--log-to-file"
    ]
    
    if mode == "remote":
        run_cluster_cmd.extend([
            "--local-log-dir", str(logs_dir / "cluster_logs")
        ])
    
    print(f"    Command: {' '.join(run_cluster_cmd)}")
    print(f"    Output redirected to: {run_cluster_log}")
    
    with open(run_cluster_log, "w") as cluster_log_file:
        cluster_process = subprocess.Popen(
            run_cluster_cmd,
            stdout=cluster_log_file,
            stderr=subprocess.STDOUT,
            stdin=subprocess.PIPE,
            text=True
        )
    
    print(f"    Cluster process started (PID: {cluster_process.pid})")
    
    sleep(5)
    
    if cluster_process.poll() is not None:
        print(f"    Error: Cluster process terminated unexpectedly!")
        print(f"    Check log file: {run_cluster_log}")
        sys.exit(1)
    
    print("    Cluster is running\n")
    
    print("Step 2: Running benchmarks...")
    benchmark_cmd = [
        python_exec,
        "-u",
        str(benchmark_script),
        "--config-file", str(benchmark_config_json),
        "--experiment-dir", str(experiments_dir),
        "--mode", mode
    ]
    
    print(f"    Command: {' '.join(benchmark_cmd)}")
    print(f"    Output redirected to: {benchmark_log}")
    
    with open(benchmark_log, "w") as bench_log_file:
        benchmark_process = subprocess.Popen(
            benchmark_cmd,
            stdout=bench_log_file,
            stderr=subprocess.STDOUT,
            text=True
        )
    
    print(f"    Benchmark process started (PID: {benchmark_process.pid})")
    print("    Waiting for benchmarks to complete...")
    
    benchmark_returncode = benchmark_process.wait()
    
    if benchmark_returncode == 0:
        print("    Benchmarks completed successfully\n")
    else:
        print(f"    Warning: Benchmark process exited with code {benchmark_returncode}")
        print(f"    Check log file: {benchmark_log}\n")
    
    print("Step 3: Cleaning up cluster...")
    print("    Sending exit signal to cluster (simulating key press)...")
    
    try:
        # send newline to cluster process to terminate and copy logs
        if cluster_process.stdin:
            cluster_process.stdin.write("\n")
            cluster_process.stdin.flush()
        
        print("    Waiting for cluster to cleanup and copy logs...")
        # wait for cluster to finish cleanup
        cluster_returncode = cluster_process.wait(timeout=60)
        
        if cluster_returncode == 0:
            print("    Cluster shutdown completed successfully")
        else:
            print(f"    Cluster exited with code {cluster_returncode}")
            
    except subprocess.TimeoutExpired:
        print("    Warning: Cluster cleanup timed out after 60 seconds")
        print("    Forcing termination...")
        cluster_process.kill()
        cluster_process.wait()
        print("    Cluster killed")
    except Exception as e:
        print(f"    Warning: Error during cleanup: {e}")
        try:
            cluster_process.terminate()
            cluster_process.wait(timeout=5)
        except:
            cluster_process.kill()
    
    print(f"\n{'='*60}")
    print(f"BENCHMARK RUN COMPLETED")
    print(f"{'='*60}")
    print(f"Results Location:")
    print(f"  - Main Directory: {benchmark_dir}")
    print(f"  - Experiments: {experiments_dir}")
    print(f"  - Logs: {logs_dir}")
    print(f"    - Cluster Log: {run_cluster_log}")
    print(f"    - Benchmark Log: {benchmark_log}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run WoolMilk benchmarks with coordinated cluster and benchmark execution."
    )
    parser.add_argument(
        "--config-folder",
        type=str,
        required=True,
        help="Path to folder containing config.json and benchmark-config.json"
    )
    parser.add_argument(
        "--mode",
        choices=["local", "remote"],
        default="local",
        help="Execution mode: local or remote"
    )
    parser.add_argument(
        "--name",
        type=str,
        required=True,
        help="Name of the benchmark (used for directory naming)"
    )
    
    args = parser.parse_args()
    
    config_folder_path = Path(args.config_folder).resolve()
    
    run_benchmarks(config_folder_path, args.mode, args.name)
