from typing import List
from fastapi import APIRouter, File, Form, UploadFile
from services.local_node_setup import (
    setup_local_entry,
    setup_local_processor,
)
import time

router = APIRouter()

@router.post("/entry-node")
async def create_entry_local(
    name: str = Form(...),
    node_files: List[UploadFile] = File(...),
    parquet_files: str | None = Form(None),
    env_path: str = Form(...),
):
    node_dir, parquet_map, log_path = setup_local_entry(
        name=name,
        uploads=node_files,
        parquet_json=parquet_files,
        env_path=env_path,
    )
    msg = (
        f"Entry node '{name}' created at {node_dir}; "
        f"parquet_files={parquet_map}.  "
        f"Logs: {log_path}" if log_path else ""
    )
    return {"message": msg}

@router.post("/processor-node")
async def create_processor_local(
    name: str = Form(...),
    env_path: str = Form(...),
):
    node_dir, log_path = setup_local_processor(
        name=name,
        env_path=env_path,
    )
    msg = (
        f"Processor node '{name}' created at {node_dir}.  "
        f"Logs: {log_path}" if log_path else ""
    )
    return {"message": msg}


@router.post("/full")
async def create_full_local(
    entry_name: str = Form(...),
    node_files: List[UploadFile] = File(...),
    parquet_files: str | None = Form(None),
    processor_name: str = Form(...),
    env_path: str = Form(...),
):
    entry_dir, parquet_map, entry_log = setup_local_entry(
        name=entry_name,
        uploads=node_files,
        parquet_json=parquet_files,
        env_path=env_path,
    )

    time.sleep(2)

    proc_dir, proc_log = setup_local_processor(
        name=processor_name,
        env_path=env_path,
    )

    msg = (
        f"Entry node '{entry_name}' created at {entry_dir}; "
        f"parquet_files={parquet_map}.  Logs: {entry_log}\n"
        f"Processor node '{processor_name}' created at {proc_dir}.  "
        f"Logs: {proc_log}"
    )
    return {"message": msg}