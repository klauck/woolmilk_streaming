# experiment

A small CLI that runs a WoolMilk benchmark end to end, detached, so it survives the
terminal closing. One command brings up the cluster, runs all experiments, and tears
the cluster down. Each run is recorded on disk, so you can check progress or pull
results later from a fresh terminal.

It does not reimplement anything: the cluster is started/held/torn down by
`run_cluster.py --headless`, and the experiments are run by `woolmilk.benchmark`.
This CLI is the glue around them.

## Commands

Run from the repo root.

```
python scripts/experiment/experiment.py run    --config-dir DIR [--jump ALIAS --repo-dir PATH --venv PATH] [--name NAME]
python scripts/experiment/experiment.py status [RUN_ID]
python scripts/experiment/experiment.py logs   RUN_ID
python scripts/experiment/experiment.py stop   RUN_ID
```

- `run` — preflight the node ports, start the cluster, run the experiments, tear down.
  Prints a `RUN_ID`.
- `status` — progress of one run (`RUN_ID`), or a table of all runs (no id).
- `logs` — copy the run's logs + result parquet into `DIR/results`.
- `stop` — stop a running experiment and free the ports.

### run options

| option | required | meaning |
|---|---|---|
| `--config-dir DIR` | yes | directory holding `benchmark.json` + `cluster.json` |
| `--jump ALIAS` | no | ssh alias of the jump host; omit to run on this machine |
| `--repo-dir PATH` | with `--jump` | repo path on the jump host |
| `--venv PATH` | with `--jump` | venv on the jump host, relative to `--repo-dir` |
| `--name NAME` | no | run label (default: the config-dir name) |

Local vs jump is the only difference between runs, and it is entirely captured by the
`Conn` (see below): omit `--jump` to execute on this machine, pass it to execute on an
on-LAN control node over ssh (ssh must work without a password).

### Setting up a jump host

`--jump duck` is just an ssh alias. It must connect without prompting for a password,
because the run is detached and nothing can type one. Set it up once, the same way the
`duck` host was added:

```bash
# 1. have an ssh key (reuse an existing one, or create it)
ssh-keygen -t ed25519            # press enter through the prompts

# 2. copy your public key to the host (asks for the password this one time)
ssh-copy-id user@192.168.2.63

# 3. give it a short alias in ~/.ssh/config
cat >> ~/.ssh/config <<'EOF'
Host duck
    HostName 192.168.2.63
    User user
    IdentityFile ~/.ssh/id_ed25519
EOF
```

After this, `ssh duck` logs in with no password, so `--jump duck` works. Add more hosts
the same way and pass whichever alias you need.

## Example

```
# on the jump host (duck), full lifecycle, detached
python scripts/experiment/experiment.py run \
    --config-dir experiment-configurations/hop2-111 \
    --jump duck --repo-dir /home/user/woolmilk_streaming --venv .venv

python scripts/experiment/experiment.py status <RUN_ID>   # experiments [####----] 12/36 done
python scripts/experiment/experiment.py logs   <RUN_ID>   # -> config-dir/results
python scripts/experiment/experiment.py stop   <RUN_ID>   # if you need to end it early
```

## What a run does

`run` launches one detached shell wrapper (built in `runner.py`):

```
run_cluster.py --headless &   # start sink + proc, hold them, remember PID
benchmark                     # run every experiment (waits for the cluster to be ready)
kill -TERM <run_cluster>      # benchmark done -> run_cluster.cleanup() frees the ports
```

- The cluster readiness race is handled by `benchmark` itself: it polls the cluster
  nodes until they report IDLE before sending any data.
- Teardown is `run_cluster`'s own `cleanup()`, triggered by SIGTERM. `stop` sends the
  same signal; a normal finish sends it from the wrapper.

## Config directory

`--config-dir` must contain:

- `benchmark.json` — source nodes, experiments, per-node config, `remote_servers`.
- `cluster.json` — sink + processing nodes (no source).

Generate them with `scripts/generate-config.py`. Node logs and result parquet are
written under the run dir and copied into `DIR/results` by `logs`.

## Where state lives

- `experiment-runs/runs.json` — the registry: one record per run (status, pid, mode,
  progress, paths). This CLI owns it; you do not edit it.
- `experiment-runs/<run-id>/` — the run's staged config, `console.log`, `cluster.log`,
  and `results/` on the executing host.

`experiment-runs/` is git-ignored.

Status is computed on demand: no daemon. `status` tails the run's `console.log`, parses
the latest `experiment i/N` and `iteration j/M`, checks the pid with `kill -0`, and
looks for the `WM_EXIT=<code>` marker to decide done/failed. Runs that are already
done/failed/stopped are read straight from the registry without touching the cluster.

## Modules

| file | responsibility |
|---|---|
| `experiment.py` | CLI: argparse + the four commands |
| `connection.py` | `Conn` = how to reach the executing host (`LocalConn` / `JumpConn`) |
| `config.py` | read `benchmark.json` / `cluster.json`, node addresses, ssh targets |
| `cluster.py` | preflight ports, and `stop` |
| `runner.py` | build the wrapper and launch it detached |
| `registry.py` | read/write `runs.json` |
| `progress.py` | parse the console log into status/progress and render it |

Everything that differs between local and jump lives in `Conn` (run a command, spawn a
detached process, stage a file, fetch results, paths). The rest of the code calls the
`Conn` and never branches on the mode.

## Requirements

- `run_cluster.py` must support `--headless` (start the cluster and hold until
  signalled). It is deployed on the host that runs the benchmark.
- For `--jump`, `ssh <alias>` must work non-interactively (key auth).
- The source dataset must be cached on the source node (5M / 10M / 20M tuples); the
  `nexmark` generator binary is not installed on the Pis, so uncached sizes will hang.
