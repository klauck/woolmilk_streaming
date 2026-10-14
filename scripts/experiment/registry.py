"""The run registry: a JSON array of run records under experiment-runs/, the
script's own store (the user never edits it)."""
import json
from pathlib import Path

REGISTRY_DIR = Path("experiment-runs")
REGISTRY_FILE = REGISTRY_DIR / "runs.json"


def load_runs():
    if not REGISTRY_FILE.exists():
        return []
    return json.loads(REGISTRY_FILE.read_text())


def save_runs(runs):
    REGISTRY_DIR.mkdir(parents=True, exist_ok=True)
    REGISTRY_FILE.write_text(json.dumps(runs, indent=2))


def find_run(runs, run_id):
    for run in runs:
        if run["id"] == run_id:
            return run
    return None


def upsert_run(run):
    runs = load_runs()
    existing = find_run(runs, run["id"])
    if existing:
        existing.update(run)
    else:
        runs.append(run)
    save_runs(runs)
