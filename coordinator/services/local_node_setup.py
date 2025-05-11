from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Dict, List, Tuple
import uuid
from schemas.local_node import LocalEntryNodeCreate, LocalProcessorNodeCreate
from schemas.entry_node import EntryNodeCreate
from fastapi import HTTPException
from schemas.processor_node import ProcessorNodeCreate
from services.entry_node import create_entry_node_without_deploy
from services.processor_node import create_processor_node_without_deploy
import tempfile
import os
from sqlalchemy.orm import Session
import sys

SERVICE_DIR = Path(__file__).resolve().parent
ENTRY_SCRIPT_SRC = SERVICE_DIR / "entry-node-files" / "node-server.py"
PROCESSOR_SCRIPT_SRC = (
    SERVICE_DIR / "processor-node-files" / "main.py"
)

def infer_parquet(files: List[str]) -> Dict[str, str]:
    files = [Path(f) for f in files]
    return {p.stem: p.name for p in files if p.suffix == ".parquet"}

def python_from_env(env_path: str) -> str:
    env = Path(env_path).expanduser()
     
    if sys.platform == "win32":
        candidates = [
            env / "Scripts" / "python.exe",
            env / "Scripts" / "python" 
        ]
    else:
        candidates = [
            env / "bin" / "python",
            env / "bin" / "python3" 
        ]
    
    for py in candidates:
        if py.exists():
            return str(py)
    
    # If none found, error out
    searched = ", ".join(str(p) for p in candidates)
    raise HTTPException(
        status_code=400,
        detail=(
            f"env_path '{env_path}' does not contain a Python interpreter. "
            f"Searched for: {searched}"
        ),
    )



def setup_local_entry(db: Session, data: LocalEntryNodeCreate) -> Tuple[Path, Dict[str, str], Path | None]:
    parquet_map = data.parquet_files_config

    parquet_arg = ",".join(f"{k}={v}" for k, v in parquet_map.items())

    python_bin = python_from_env(data.env_name)

    fd, log_path = tempfile.mkstemp(prefix='coordinator_', suffix='.log')
    os.close(fd)

    log_path = Path(log_path)

    with log_path.open("a") as log:
        log.write(f"parquet_files={parquet_arg}\n")

    cmd = [
        "nohup",
        python_bin,
        str(ENTRY_SCRIPT_SRC),
        "--parquet_files",
        parquet_arg,
        "--host",
        data.serving_host,
        "--port",
        str(data.serving_port),
    ]

    with log_path.open("a") as log:
        popen_kwargs = dict(
            stdout=log,
            stderr=subprocess.STDOUT,
            close_fds=True,
        )

        if sys.platform == "win32":
            # On Windows, use CREATE_NEW_PROCESS_GROUP
            popen_kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
        else:
            # On Unix, use setsid() via preexec_fn
            popen_kwargs["preexec_fn"] = os.setsid

        subprocess.Popen(cmd, **popen_kwargs)

    node_data_dict = {
        "name": data.name,
        "ssh_host": "",
        "ssh_user": "",
        "ssh_port": 0,
        "ssh_password": "",
        "serving_host": data.serving_host,
        "serving_port": data.serving_port,
        "parquet_files": parquet_map,
        "env_name": data.env_name,
        "bit_rate": data.bit_rate,
    }

    node_data = EntryNodeCreate(**node_data_dict)

    create_entry_node_without_deploy(db=db, node_data=node_data, status="running", 
                                    status_message="Node created with logs at "+ str(log_path)+". Parquet files: "+str(parquet_map)+".")

    return parquet_map, log_path


def setup_local_processor(
    db: Session, data: LocalProcessorNodeCreate
) -> Tuple[Path, Path | None]:
    fd, log_path = tempfile.mkstemp(prefix='processor_', suffix='.log')
    os.close(fd)

    log_path = Path(log_path)

    if data.name is None:
        data.name = str(uuid.uuid4())

    with log_path.open("a") as log:
        log.write(f"processor node logs start\n")

    python_bin = python_from_env(data.env_name)

    cmd = ["nohup", python_bin, str(PROCESSOR_SCRIPT_SRC)]

    if data.exit_host:
        cmd += ["--exit-host", data.exit_host]
    
    if data.exit_port:
        cmd += ["--exit-port", str(data.exit_port)]

    cmd += [
        "--host",
        data.serving_host,
        "--port",
        str(data.serving_port),
        "--node-id",
        data.name
    ]

    with log_path.open("a") as log:
        log.write(f"Command: {' '.join(cmd)}\n")
        subprocess.Popen(
            cmd, stdout=log, stderr=subprocess.STDOUT, close_fds=True, preexec_fn=os.setsid
        )

    node_data_dict = {
        "name": data.name,
        "ssh_host": "",
        "ssh_user": "",
        "ssh_port": 0,
        "ssh_password": "",
        "exit_host": data.exit_host,
        "exit_port": data.exit_port,
        "serving_host": data.serving_host,
        "serving_port": data.serving_port,
        "env_name": data.env_name,
    }

    print("Node data dict:", node_data_dict)

    node_data = ProcessorNodeCreate(**node_data_dict)

    create_processor_node_without_deploy(
        db=db,
        node_data=node_data,
        status="running",
        status_message="Node created with logs at "+ str(log_path)+".",
    )

    return log_path
