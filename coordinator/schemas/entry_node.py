from pydantic import BaseModel
from typing import Optional, Dict

class EntryNodeBase(BaseModel):
    """
    Common fields for creating and updating an entry node.
    """
    name: str
    ssh_host: str
    ssh_user: str
    ssh_port: int
    ssh_password: Optional[str] = None
    serving_host: str
    serving_port: int
    parquet_files: Optional[Dict[str, str]] = None
    env_name: str  # New field for environment name
    bit_rate: int = 0  # New field for bit rate

class EntryNodeCreate(EntryNodeBase):
    """
    Used for creating a new entry node.
    """
    pass

class EntryNodeUpdate(BaseModel):
    """
    Used if you want to allow partial updates (PUT/PATCH).
    """
    name: Optional[str] = None
    ssh_host: Optional[str] = None
    ssh_user: Optional[str] = None
    ssh_port: Optional[int] = None
    ssh_password: Optional[str] = None
    serving_host: Optional[str] = None
    serving_port: Optional[int] = None
    parquet_files: Optional[Dict[str, str]] = None
    env_name: Optional[str] = None  # New field added for updates
    status: Optional[str] = None
    status_message: Optional[str] = None

class EntryNodeOut(EntryNodeBase):
    """
    Response model that includes database ID and status fields.
    """
    id: int
    status: str
    status_message: str

    class Config:
        orm_mode = True 