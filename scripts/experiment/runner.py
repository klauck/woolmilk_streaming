"""Launching a run detached. A run is a small shell wrapper that drives the two
existing tools, the same everywhere; the Conn decides where it runs:

  1. run_cluster.py --headless  -> starts the cluster (sink + proc), holds it
  2. woolmilk.benchmark         -> runs all experiments against it
  3. kill the run_cluster process -> its signal handler calls cleanup() (teardown)

Paths are relative to the repo (the run's cwd) so the per-node log paths the
benchmark hands to the Pis resolve under each Pi's base_dir. A WM_EXIT=<code>
marker is appended to the console log when the benchmark exits."""


def build_wrapper(python, rel):
    cluster_cmd = (
        f"{python} -u -m woolmilk.run_cluster"
        f" --config {rel}/cluster.json --mode remote --headless"
    )
    bench_cmd = (
        f"{python} -u -m woolmilk.benchmark"
        f" --config-file {rel}/benchmark.json --experiment-dir {rel}/results --mode remote"
    )
    return (
        f"{cluster_cmd} >> {rel}/cluster.log 2>&1 & RC=$!; "
        f"{bench_cmd} >> {rel}/console.log 2>&1; echo WM_EXIT=$? >> {rel}/console.log; "
        f"kill -TERM $RC"
    )


def launch(conn, run_id, config_dir):
    conn.mkdir(f"{conn.run_dir(run_id)}/results")
    conn.stage(config_dir / "benchmark.json", run_id, "benchmark.json")
    conn.stage(config_dir / "cluster.json", run_id, "cluster.json")
    script = build_wrapper(conn.python, conn.rel_dir(run_id))
    return conn.spawn_detached(script)
