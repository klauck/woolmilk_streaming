"""Deriving a run's live progress and status from its console log, and rendering
it for the terminal. No daemon: everything is parsed on demand."""
import re
from datetime import datetime

from connection import conn_from_run
from registry import upsert_run

RE_EXPERIMENT = re.compile(r"Starting experiment (\d+)/(\d+)")
RE_ITERATION = re.compile(r"Starting iteration (\d+)/(\d+)")
RE_EXIT = re.compile(r"WM_EXIT=(-?\d+)")
FAIL_MARKERS = ("Traceback (most recent call last)", "Timeout reached")


def read_console(run, tail=400):
    conn = conn_from_run(run)
    result = conn.run(f"tail -n {tail} {run['console_log']}", capture_output=True, text=True)
    return result.stdout


def is_alive(run):
    pid = run.get("pid")
    if not pid:
        return False
    conn = conn_from_run(run)
    return conn.run(f"kill -0 {pid}", capture_output=True).returncode == 0


def last_match(pattern, text):
    matches = pattern.findall(text)
    return matches[-1] if matches else None


def parse_progress(text):
    progress = {}
    exp = last_match(RE_EXPERIMENT, text)
    itr = last_match(RE_ITERATION, text)
    if exp:
        progress["experiment"] = f"{exp[0]}/{exp[1]}"
    if itr:
        progress["iteration"] = f"{itr[0]}/{itr[1]}"
    return progress


def _now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def refresh(run):
    if run["status"] in ("done", "failed", "stopped"):
        return run

    text = read_console(run)
    run["progress"] = parse_progress(text) or run.get("progress", {})

    exit_match = last_match(RE_EXIT, text)
    if exit_match is not None:
        run["status"] = "done" if int(exit_match) == 0 else "failed"
        run["finished"] = run.get("finished") or _now()
    elif any(marker in text for marker in FAIL_MARKERS):
        run["status"] = "failed"
    elif is_alive(run):
        run["status"] = "running"
    else:
        run["status"] = "failed"
        run["finished"] = run.get("finished") or _now()

    upsert_run(run)
    return run


def split_fraction(value):
    try:
        cur, total = value.split("/")
        return int(cur), int(total)
    except (AttributeError, ValueError):
        return None


def bar(cur, total, width=20):
    filled = int(width * cur / total) if total else 0
    return "[" + "#" * filled + "-" * (width - filled) + "]"


def fmt_progress(progress):
    return f"exp {progress.get('experiment', '-')} itr {progress.get('iteration', '-')}"


def print_detail(run):
    print(f"{run['id']}   [{run['status']}]")
    target = f" ({run['jump']})" if run["jump"] else ""
    print(f"  mode       {run['mode']}{target}")
    print(f"  started    {run['created']}")
    print(f"  pid        {run['pid']}")

    exp = split_fraction(run["progress"].get("experiment"))
    itr = split_fraction(run["progress"].get("iteration"))
    if exp:
        cur, total = exp
        done = total if run["status"] == "done" else cur - 1
        print(f"  experiments {bar(done, total)} {done}/{total} done (on {cur})")
    else:
        print(f"  experiments -/{run['total_experiments']} (not started)")
    if itr:
        print(f"  iterations  {itr[0]}/{itr[1]} of current experiment")


def print_table(runs):
    print(f"{'RUN_ID':30} {'MODE':6} {'STATUS':8} {'PROGRESS':18} CREATED")
    for run in runs:
        run = refresh(run)
        print(
            f"{run['id']:30} {run['mode']:6} {run['status']:8} "
            f"{fmt_progress(run['progress']):18} {run['created']}"
        )
