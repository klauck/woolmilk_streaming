from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List
import socket

from database import get_db
from services.processor_node import (
    create_processor_node,
    get_processor_node,
    list_processor_nodes,
    delete_processor_node
)
from schemas.processor_node import ProcessorNodeCreate, ProcessorNodeOut

router = APIRouter()

@router.post("/processor-node", response_model=ProcessorNodeOut)
def create_node(node_in: ProcessorNodeCreate, db: Session = Depends(get_db)):
    """
    Create a processor node in the database and deploy it remotely.
    """
    node = create_processor_node(db, node_in)
    return node

@router.get("/processor-node", response_model=List[ProcessorNodeOut])
def list_nodes(db: Session = Depends(get_db)):
    """
    List all processor nodes in the database.
    """
    return list_processor_nodes(db)

@router.get("/processor-node/{node_id}", response_model=ProcessorNodeOut)
def get_node(node_id: int, db: Session = Depends(get_db)):
    """
    Get a specific processor node by ID.
    """
    node = get_processor_node(db, node_id)
    if not node:
        raise HTTPException(status_code=404, detail="Processor node not found")
    return node

@router.delete("/processor-node/{node_id}", response_model=dict)
def delete_node(node_id: int, db: Session = Depends(get_db)):
    """
    Delete a processor node by ID.
    """
    node = get_processor_node(db, node_id)
    if not node:
        raise HTTPException(status_code=404, detail="Processor node not found")
    delete_processor_node(db, node)
    return {"detail": f"Processor node {node.name} deleted."}

@router.get("/processor-node/{node_id}/status")
def check_node_status(node_id: int, db: Session = Depends(get_db)):
    """
    Check the status of a processor node by attempting to connect to its exit node port.
    """
    node = get_processor_node(db, node_id)
    if not node:
        raise HTTPException(status_code=404, detail="Processor node not found")

    host_to_check = node.exit_host
    port_to_check = node.exit_port

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(2.0)
    try:
        s.connect((host_to_check, port_to_check))
        s.close()
        return {
            "node_id": node.id,
            "name": node.name,
            "port_status": "open",
            "message": f"Port {port_to_check} on {host_to_check} is open. Processor node is working."
        }
    except Exception:
        return {
            "node_id": node.id,
            "name": node.name,
            "port_status": "closed",
            "message": f"Port {port_to_check} on {host_to_check} is closed. Processor node is not working."
        }
