from pydantic import BaseModel
from typing import Optional, List, Any

class ProcessorNodeBase(BaseModel):
    name: str
    ssh_host: str
    ssh_user: str
    ssh_port: int
    ssh_password: Optional[str] = None
    ssh_password: Optional[str] = None
    exit_host: Optional[str] = None  # Now optional, default is None
    exit_port: Optional[int] = None    # Now optional, default is None
    entry_endpoints: Optional[Any] = None

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
    entry_endpoints: Optional[Any] = None
    status: Optional[str] = None
    status_message: Optional[str] = None

class ProcessorNodeOut(ProcessorNodeBase):
    id: int
    status: str
    status_message: str

    class Config:
        orm_mode = True
