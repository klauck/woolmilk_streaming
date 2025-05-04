from typing import List
from fastapi import APIRouter, File, Form, UploadFile, Depends
from sqlalchemy.orm import Session
from services.local_node_setup import (
    setup_local_entry,
    setup_local_processor,
)
from schemas.local_node import LocalEntryNodeCreate, LocalProcessorNodeCreate
from database import get_db
import time
import uuid

router = APIRouter()

@router.post("/entry-node")
async def create_entry_node_local(local_entry_node_data: LocalEntryNodeCreate, db: Session = Depends(get_db)):
    if local_entry_node_data.name is None:
        local_entry_node_data.name = str(uuid.uuid4())

    parquet_map, log_path = setup_local_entry(db, local_entry_node_data)
    msg = (
        f"Entry node '{local_entry_node_data.name}' created."
        f"parquet_files={parquet_map}.  "
        f"Logs: {log_path}" if log_path else ""
    )
    return {"message": msg}

@router.post("/processor-node")
async def create_processor_local(local_processor_node: LocalProcessorNodeCreate, db: Session = Depends(get_db)):
    if local_processor_node.name is None:
        local_processor_node.name = str(uuid.uuid4())

    log_path = setup_local_processor(
        db=db,
        data=local_processor_node,
        launch=True,
    )

    msg = (
        f"Processor node '{local_processor_node.name}' created.  "
        f"Logs: {log_path}" if log_path else ""
    )
    
    return {"message": msg}