"""Detached experiment orchestrator for WoolMilk benchmarks.

Runs a whole experiment end to end, detached, so it survives the terminal closing:
preflight the node ports, start the cluster (sink + processing), launch the
benchmark (source + per-experiment config), then tear the cluster down. Each run is
recorded under experiment-runs/ so status and logs work later from a fresh terminal.

    experiment.py run    --config-dir DIR [--jump ALIAS --repo-dir PATH --venv PATH] [--name NAME]
    experiment.py status [RUN_ID]
    experiment.py logs   RUN_ID
    experiment.py stop   RUN_ID

Run it from the repo root. A config directory holds benchmark.json + cluster.json;
node logs and result parquet land in <config-dir>/results.
"""
import argparse
import sys
from datetime import datetime
from pathlib import Path

import cluster
import config
import progress
import runner
from connection import conn_from_run, make_conn
from registry import REGISTRY_DIR, find_run, load_runs, upsert_run


def cmd_run(args):
    config_dir = Path(args.config_dir).resolve()
    bench = config.read_json(config_dir, "benchmark.json")
    cluster_cfg = config.read_json(config_dir, "cluster.json")
    servers = config.remote_servers(bench)

    conn = make_conn(args.jump, args.repo_dir, args.venv)
    mode = "jump" if args.jump else "local"
    name = args.name or config_dir.name
    run_id = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{name}"

    endpoints = config.source_endpoints(bench) + config.cluster_endpoints(cluster_cfg)
    cluster.preflight(conn, servers, endpoints)

    REGISTRY_DIR.mkdir(parents=True, exist_ok=True)
    pid = runner.launch(conn, run_id, config_dir)

    upsert_run({
        "id": run_id,
        "name": name,
        "created": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "status": "running",
        "mode": mode,
        "jump": args.jump,
        "repo_dir": conn.repo,
        "venv": args.venv,
        "config_dir": str(config_dir),
        "results_dir": str(config_dir / "results"),
        "console_log": conn.console_log(run_id),
        "pid": pid,
        "total_experiments": len(bench.get("experiments", [])),
        "progress": {},
        "finished": None,
    })

    print(f"\nstarted run {run_id} ({mode}, pid {pid})")
    print(f"    experiment.py status {run_id}")
    print(f"    experiment.py logs   {run_id}")


def cmd_status(args):
    runs = load_runs()
    if not runs:
        print("no runs recorded")
        return

    if args.run_id:
        run = find_run(runs, args.run_id)
        if not run:
            sys.exit(f"error: run {args.run_id} not found")
        progress.print_detail(progress.refresh(run))
        return

    progress.print_table(runs)


def cmd_logs(args):
    runs = load_runs()
    run = find_run(runs, args.run_id)
    if not run:
        sys.exit(f"error: run {args.run_id} not found")

    run = progress.refresh(run)
    if run["status"] == "running":
        answer = input(
            "run is still active -- fetching over the cluster network may skew "
            "throughput. continue? [y/N] "
        ).strip().lower()
        if answer != "y":
            sys.exit("aborted")

    results_dir = run["results_dir"]
    conn_from_run(run).fetch_results(run["id"], results_dir)
    print(f"fetched results into {results_dir}")


def cmd_stop(args):
    runs = load_runs()
    run = find_run(runs, args.run_id)
    if not run:
        sys.exit(f"error: run {args.run_id} not found")

    bench = config.read_json(Path(run["config_dir"]), "benchmark.json")
    servers = config.remote_servers(bench)
    cluster.stop(conn_from_run(run), run["id"], config.source_endpoints(bench), servers)

    run["status"] = "stopped"
    run["finished"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    upsert_run(run)
    print(f"stopped {run['id']}")


def build_parser():
    parser = argparse.ArgumentParser(description="WoolMilk experiment orchestrator")
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="launch a benchmark end to end, detached")
    run.add_argument("--config-dir", required=True, help="dir with benchmark.json + cluster.json")
    run.add_argument("--jump", help="ssh alias of jump host (omit to run locally)")
    run.add_argument("--repo-dir", help="repo path on the jump host (required with --jump)")
    run.add_argument("--venv", help="venv on the jump host, relative to --repo-dir (required with --jump)")
    run.add_argument("--name", help="run label (default: config-dir name)")
    run.set_defaults(func=cmd_run)

    status = sub.add_parser("status", help="show run progress")
    status.add_argument("run_id", nargs="?", help="run id (omit for a table of all runs)")
    status.set_defaults(func=cmd_status)

    logs = sub.add_parser("logs", help="fetch all logs + results into the config dir")
    logs.add_argument("run_id", help="run id")
    logs.set_defaults(func=cmd_logs)

    stop = sub.add_parser("stop", help="stop a running experiment and tear the cluster down")
    stop.add_argument("run_id", help="run id")
    stop.set_defaults(func=cmd_stop)

    return parser


def main():
    args = build_parser().parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
