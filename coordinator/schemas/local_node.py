from pydantic import BaseModel
from typing import Optional, Dict
import uuid

class LocalEntryNodeBase(BaseModel):
    """
    Common fields for creating and updating an local entry node.
    """
    name: Optional[str] = None
    serving_host: Optional[str] = "127.0.0.1"
    serving_port: Optional[int] = 8815
    parquet_files_config: Dict[str, str] = {
        "bids": "/Users/usamabintariq/Documents/GitHub/woolmilk_streaming/data/bids.parquet"
    }
    env_name: str = "/Users/usamabintariq/Documents/GitHub/woolmilk_streaming/test_env"
    bit_rate: Optional[int] = 100000

class LocalEntryNodeCreate(LocalEntryNodeBase):
    """
    Used for creating a new entry local node.
    """
    pass