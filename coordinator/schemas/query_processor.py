from typing import List
from pydantic import BaseModel

class EntryEndpoint(BaseModel):
    name: str
    host: str
    port: str
    query_names: List[str]

class EntryEndpointWithID(BaseModel):
    entry_node_name: str
    queries_to_run: List[str]

class Query(BaseModel):
    name: str
    query: str

class AddQueryRequestModel(BaseModel):
    entry_endpoint_ids: List[EntryEndpointWithID] = [{
        "entry_node_name": "endpoint_1",
        "queries_to_run": ["query_1"]
    }]
    
    queries: List[Query] = [{
        "name": "query_1",
        "query": "SELECT * FROM bids"
    }]

    node_id: str = "node_1"