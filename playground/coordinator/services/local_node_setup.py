from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path
from typing import Dict, List, Tuple

from fastapi import HTTPException, UploadFile

BASE_DIR = Path("./local_node_setup/").expanduser()
BASE_DIR.mkdir(parents=True, exist_ok=True)

SERVICE_DIR = Path(__file__).resolve().parent
ENTRY_SCRIPT_SRC = SERVICE_DIR / "entry-node-files" / "node-server.py"
PROCESSOR_SCRIPT_SRC = (
    SERVICE_DIR / "processor-node-files" / "processor-node.py"
)

def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)

def save_uploads(target: Path, uploads: List[UploadFile]) -> List[Path]:
    saved: List[Path] = []
    for up in uploads:
        dest = target / up.filename
        with dest.open("wb") as dst:
            shutil.copyfileobj(up.file, dst)
        saved.append(dest)
    return saved

def infer_parquet(files: List[Path]) -> Dict[str, str]:
    return {p.stem: p.name for p in files if p.suffix == ".parquet"}


def python_from_env(env_path: str) -> str:
    env = Path(env_path).expanduser()
    bin_dir = env / "bin"
    py = bin_dir / "python"

    if not py.exists():
        raise HTTPException(
            400,
            f"env_path '{env_path}' does not contain a Python interpreter",
        )
    return str(py)


def setup_local_entry(
    name: str,
    uploads: List[UploadFile],
    parquet_json: str | None,
    env_path: str,
    start: bool = True,
) -> Tuple[Path, Dict[str, str], Path | None]:
    node_dir = BASE_DIR / "entry" / name
    ensure_dir(node_dir)

    saved = save_uploads(node_dir, uploads)
    shutil.copy2(ENTRY_SCRIPT_SRC, node_dir / "node_server.py")

    if parquet_json:
        try:
            parquet_map = json.loads(parquet_json)
            if not isinstance(parquet_map, dict):
                raise ValueError
        except Exception:
            raise HTTPException(400, "parquet_files must be a JSON object")
    else:
        parquet_map = infer_parquet(saved)

    log_path = None
    if start:
        log_path = launch_entry_server(node_dir, parquet_map, env_path)

    return node_dir, parquet_map, log_path


def setup_local_processor(
    name: str,
    env_path: str,
    start: bool = True,
) -> Tuple[Path, Path | None]:
    node_dir = BASE_DIR / "processor" / name
    ensure_dir(node_dir)

    shutil.copy2(PROCESSOR_SCRIPT_SRC, node_dir / "processor_node.py")

    log_path = None
    if start:
        log_path = launch_processor(node_dir, env_path)

    return node_dir, log_path

def launch_entry_server(
    node_dir: Path,
    parquet_map: Dict[str, str],
    env_path: str,
) -> Path:
    script = node_dir / "node_server.py"
    parquet_arg = ",".join(f"{k}={v}" for k, v in parquet_map.items())
    log_path = node_dir / "entry_stdout.log"
    python_bin = python_from_env(env_path)

    with log_path.open("a") as log:
        log.write(f"parquet_files={parquet_arg}\n")

    cmd = [
        "nohup",
        python_bin,
        str(script),
        "--data_dir",
        str(node_dir),
        "--parquet_files",
        parquet_arg,
    ]
    with log_path.open("a") as log:
        subprocess.Popen(
            cmd, stdout=log, stderr=subprocess.STDOUT, close_fds=True
        )

    return log_path


def launch_processor(
    node_dir: Path,
    env_path: str,
) -> Path:
    script = node_dir / "processor_node.py"
    log_path = node_dir / "processor_stdout.log"
    python_bin = python_from_env(env_path)

    cmd = ["nohup", python_bin, str(script)]
    with log_path.open("a") as log:
        subprocess.Popen(
            cmd, stdout=log, stderr=subprocess.STDOUT, close_fds=True
        )

    return log_path
