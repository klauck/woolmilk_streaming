"""How to reach the host that runs the experiment: this machine (LocalConn) or an
on-LAN jump host over ssh (JumpConn). Everything else (preflight, launch, stop,
status) is written once against this interface and never branches on the mode.

A Conn knows how to: run a shell command there, spawn a detached process and
return its pid, stage a file into the run dir, fetch results back, and where the
run dir / logs live.
"""
import os
import shutil
import subprocess
import sys
from pathlib import Path

from remote import quote, run_ssh


def local_python():
    venv_python = Path.cwd() / ".venv/bin/python"
    return str(venv_python) if venv_python.exists() else sys.executable


class Conn:
    repo = ""
    python = ""

    def run(self, command, **kwargs):
        raise NotImplementedError

    def spawn_detached(self, script):
        raise NotImplementedError

    def stage(self, src, run_id, name):
        raise NotImplementedError

    def fetch_results(self, run_id, dest_dir):
        raise NotImplementedError

    def run_dir(self, run_id):
        return f"{self.repo}/experiment-runs/{run_id}"

    def rel_dir(self, run_id):
        return f"experiment-runs/{run_id}"

    def console_log(self, run_id):
        return f"{self.run_dir(run_id)}/console.log"

    def cluster_log(self, run_id):
        return f"{self.run_dir(run_id)}/cluster.log"

    def mkdir(self, path):
        self.run(f"mkdir -p {path}", check=True)


class LocalConn(Conn):
    def __init__(self, repo=None):
        self.repo = repo or str(Path.cwd())
        self.python = local_python()

    def run(self, command, **kwargs):
        return run_ssh(["sh", "-c", command], **kwargs)

    def spawn_detached(self, script):
        with open(os.devnull) as devnull:
            proc = subprocess.Popen(
                ["sh", "-c", script],
                cwd=self.repo,
                stdin=devnull,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
            )
        return proc.pid

    def stage(self, src, run_id, name):
        dest = Path(self.run_dir(run_id))
        dest.mkdir(parents=True, exist_ok=True)
        shutil.copy(src, dest / name)

    def fetch_results(self, run_id, dest_dir):
        Path(dest_dir).mkdir(parents=True, exist_ok=True)
        self.run(f"cp -r {self.run_dir(run_id)}/results/. {dest_dir}", check=True)


class JumpConn(Conn):
    def __init__(self, jump, repo, venv):
        self.jump = jump
        self.repo = repo
        self.python = f"{repo}/{venv}/bin/python"

    def run(self, command, **kwargs):
        return run_ssh(["ssh", self.jump, command], **kwargs)

    def spawn_detached(self, script):
        remote = (
            f"cd {self.repo}; "
            f"{{ setsid sh -c {quote(script)} >/dev/null 2>&1 </dev/null & }}; "
            f"echo $!"
        )
        result = run_ssh(["ssh", self.jump, remote], capture_output=True, text=True, check=True)
        return int(result.stdout.strip().splitlines()[-1])

    def stage(self, src, run_id, name):
        self.mkdir(self.run_dir(run_id))
        run_ssh(["scp", str(src), f"{self.jump}:{self.run_dir(run_id)}/{name}"], check=True)

    def fetch_results(self, run_id, dest_dir):
        Path(dest_dir).mkdir(parents=True, exist_ok=True)
        run_ssh(
            ["scp", "-r", f"{self.jump}:{self.run_dir(run_id)}/results/.", str(dest_dir)],
            check=True,
        )


def make_conn(jump, repo_dir, venv):
    if jump:
        if not repo_dir or not venv:
            sys.exit("error: --repo-dir and --venv are required with --jump")
        return JumpConn(jump, repo_dir, venv)
    return LocalConn()


def conn_from_run(run):
    if run["mode"] == "jump":
        return JumpConn(run["jump"], run["repo_dir"], run["venv"])
    return LocalConn(run["repo_dir"])
