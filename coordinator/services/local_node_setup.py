from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Dict, List, Tuple
from schemas.local_node import LocalEntryNodeCreate, LocalProcessorNodeCreate
from schemas.entry_node import EntryNodeCreate
from fastapi import HTTPException
from services.entry_node import create_entry_node_without_deploy
from services.processor_node import create_processor_node_without_deploy
import tempfile
import os
from sqlalchemy.orm import Session

SERVICE_DIR = Path(__file__).resolve().parent
ENTRY_SCRIPT_SRC = SERVICE_DIR / "entry-node-files" / "node-server.py"
PROCESSOR_SCRIPT_SRC = (
    SERVICE_DIR / "processor-node-files" / "processor-node.py"
)

def infer_parquet(files: List[str]) -> Dict[str, str]:
    files = [Path(f) for f in files]
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
        subprocess.Popen(
            cmd, stdout=log, stderr=subprocess.STDOUT, close_fds=True
        )

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
    db: Session, data: LocalProcessorNodeCreate, launch: bool = True
) -> Tuple[Path, Path | None]:
    
    entry_endpoints_str = None
    queries_str = None

    if data.queries:
        queries_str = ";".join(
            f"{q.name}|{q.query}" for q in data.queries
        )

    if data.entry_endpoints:
        entry_endpoints_str = ";".join(
            f"{e.name}|{e.host}|{e.port}|{e.query_name}" for e in data.entry_endpoints
        )
    
    fd, log_path = tempfile.mkstemp(prefix='processor_', suffix='.log')
    os.close(fd)

    log_path = Path(log_path)

    with log_path.open("a") as log:
        log.write(f"processor node logs start\n")

    if launch:
        python_bin = python_from_env(data.env_name)

        cmd = ["nohup", python_bin, str(PROCESSOR_SCRIPT_SRC)]

        if data.exit_host:
            cmd += ["--exit_host", data.exit_host]
        
        if data.exit_port:
            cmd += ["--exit_port", str(data.exit_port)]

        if entry_endpoints_str:
            entry_endpoints = entry_endpoints_str
            cmd += ["--entry_endpoints", entry_endpoints]

        if queries_str:
            queries = queries_str
            cmd += ["--queries", queries]

        with log_path.open("a") as log:
            log.write(f"Command: {' '.join(cmd)}\n")
            subprocess.Popen(
                cmd, stdout=log, stderr=subprocess.STDOUT, close_fds=True
            )

    node_data_dict = {
        "name": data.name,
        "ssh_host": "",
        "ssh_user": "",
        "ssh_port": 0,
        "ssh_password": "",
        "exit_host": data.exit_host,
        "exit_port": data.exit_port,
        "entry_endpoints": data.entry_endpoints or [],
        "queries": data.queries or [],
        "env_name": data.env_name,
    }

    print("Node data dict:", node_data_dict)

    node_data = LocalProcessorNodeCreate(**node_data_dict)

    create_processor_node_without_deploy(
        db=db,
        node_data=node_data,
        status="running",
        status_message="Node created with logs at "+ str(log_path)+".",
    )

    return log_path
