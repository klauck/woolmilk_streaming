from pydantic import BaseModel
from typing import Optional, List, Dict, Any
import uuid

class ProcessorNodeBase(BaseModel):
    name: Optional[str] = str(uuid.uuid4())
    ssh_host: str
    ssh_user: str
    ssh_port: int
    ssh_password: Optional[str] = None
    exit_host: Optional[str] = None
    exit_port: Optional[int] = None

    env_name: str

    entry_endpoints: Optional[List[Dict[str, Any]]] = None

    queries: Optional[List[Dict[str, Any]]] = None

class ProcessorNodeCreate(ProcessorNodeBase):
    pass

class ProcessorNodeUpdate(BaseModel):
    name: Optional[str] = None
    ssh_host: Optional[str] = None
    ssh_user: Optional[str] = None
    ssh_port: Optional[int] = None
    ssh_password: Optional[str] = None
    exit_host: Optional[str] = None
    exit_port: Optional[int] = None
    env_name: Optional[str] = None

    entry_endpoints: Optional[List[Dict[str, Any]]] = None
    queries: Optional[List[Dict[str, Any]]] = None

    status: Optional[str] = None
    status_message: Optional[str] = None

class ProcessorNodeOut(ProcessorNodeBase):
    id: int
    status: str
    status_message: str

    class Config:
        orm_mode = True
