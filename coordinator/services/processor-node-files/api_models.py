
# ── Pydantic models ──────────────────────────────────────────────────────
from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel

class EntryEndpoint(BaseModel):
    name: str
    host: str
    port: str
    query_names: List[str]

class Query(BaseModel):
    name: str
    query: str
    status: str = "pending"     # pending/running/finished

class QueryStatus(BaseModel):
    name: str
    query: str
    start_time: Optional[datetime]
    finished_time: Optional[datetime]
    time_taken: Optional[float]
    status: str                 # mirrors Query.status

class AddQueryRequest(BaseModel):
    entry_endpoints: List[EntryEndpoint] = [{
        "name": "endpoint_1",
        "host": "127.0.0.1",
        "port": "8815",
        "query_names": ["query_1"]
    }]
    
    queries: List[Query] = [{
        "name": "query_1",
        "query": "SELECT * FROM bids"
    }]

class QueryAddedResponse(BaseModel):
    message: str
    total_in_queue: int

class InfoResponse(BaseModel):
    node_id: str
    status: str                     # running | idle
    current_query: Optional[QueryStatus]
    all_queries: List[QueryStatus]
