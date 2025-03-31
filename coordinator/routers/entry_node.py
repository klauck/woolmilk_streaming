from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List

from database import get_db
from services.entry_node import (
    create_entry_node,
    get_entry_node,
    list_entry_nodes,
    delete_entry_node
)
from schemas.entry_node import EntryNodeCreate, EntryNodeOut
import socket

router = APIRouter()

@router.post("/entry-node", response_model=EntryNodeOut)
def create_node(node_in: EntryNodeCreate, db: Session = Depends(get_db)):
    """
    Create an entry node in the database and attempt to deploy it.
    """
    node = create_entry_node(db, node_in)
    return node

@router.get("/entry-node", response_model=List[EntryNodeOut])
def list_nodes(db: Session = Depends(get_db)):
    """
    List all entry nodes in the database.
    """
    return list_entry_nodes(db)

@router.get("/entry-node/{node_id}", response_model=EntryNodeOut)
def get_node(node_id: int, db: Session = Depends(get_db)):
    """
    Get a specific entry node by ID.
    """
    node = get_entry_node(db, node_id)
    if not node:
        raise HTTPException(status_code=404, detail="Entry node not found")
    return node

@router.delete("/entry-node/{node_id}", response_model=dict)
def delete_node(node_id: int, db: Session = Depends(get_db)):
    """
    Delete an entry node by ID.
    """
    node = get_entry_node(db, node_id)
    if not node:
        raise HTTPException(status_code=404, detail="Entry node not found")
    delete_entry_node(db, node)
    return {"detail": f"Entry node {node.name} deleted."}

@router.get("/entry-node/{node_id}/status")
def check_node_status(node_id: int, db: Session = Depends(get_db)):
    """
    Check the status of an entry node by ID.
    """
    node = get_entry_node(db, node_id)
    if not node:
        raise HTTPException(status_code=404, detail="Entry node not found")

    host_to_check = node.ssh_host
    port_to_check = node.serving_port

    # Check if the port is open 
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(2.0)

    try:
        s.connect((host_to_check, port_to_check))
        s.close()
        
        return {
            "node_id": node.id,
            "name": node.name,
            "port_status": "open",
            "message": f"Port {port_to_check} on {host_to_check} is open. Node is working."
        }
    except Exception:
        
        return {
            "node_id": node.id,
            "name": node.name,
            "port_status": "closed",
            "message": f"Port {port_to_check} on {host_to_check} is closed. Node is not working."
        }