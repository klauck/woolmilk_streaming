from typing import List
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from database import get_db
from models.entry_node import EntryNode
from models.processor_node import ProcessorNode
from models.query_processor import QueryEntry
from schemas.processor_node import EntryEndpoint, ProcessorNodeOut, Query
from schemas.query_processor import AddQueryRequestModel
import requests

router = APIRouter()

@router.post("/add")
def add_new_processor_query(query_in:AddQueryRequestModel, db:Session=Depends(get_db)):
    entry_endpoints: List[EntryEndpoint] = []

    for entry in query_in.entry_endpoint_ids:
        # get from db
        entry_node: EntryNode = db.query(EntryNode).filter(EntryNode.name == entry.entry_node_name).first()

        if not entry_node:
            return {
                "message": f"Entry node with ID {entry} not found",
                "status": "error"
            }

        entry_endpoint: EntryEndpoint = {
            "name": entry_node.name,
            "host": entry_node.serving_host,
            "port": entry_node.serving_port,
            "query_names": entry.queries_to_run
        }

        # make sure all queries to run are in the query_in.name
        for query_name in entry.queries_to_run:
            if query_name not in [query.name for query in query_in.queries]:
                return {
                    "message": f"Query {query_name} not found in the provided queries",
                    "status": "error"
                }

        entry_endpoints.append(entry_endpoint)

    queries = [q.dict() for q in query_in.queries]
    data_to_processor_node = {
        "entry_endpoints": entry_endpoints,
        "queries": queries
    }

    # get first processor node in the db
    processor_node: ProcessorNode = db.query(ProcessorNode).first()

    if not processor_node:
        return {
            "message": "No processor node found",
            "status": "error"
        }
    
    processor_node_http_host = processor_node.serving_host
    processor_node_http_port = processor_node.serving_port

    # send data_to_processor_node to the processor node at endpoint /add-query

    url = f"http://{processor_node_http_host}:{processor_node_http_port}/add-query"
    headers = {"Content-Type": "application/json"}
    response = requests.post(url, json=data_to_processor_node, headers=headers)

    if response.status_code != 200:
        print(f"Error adding query to processor node: {response.text}")
        return {
            "message": f"Error adding query to processor node: {response.text}",
            "status": "error"
        }
    
    query_entry: QueryEntry = QueryEntry(
        entry_endpoints=entry_endpoints,
        queries=queries,
        processor_node_ids=[processor_node.id]
    )

    db.add(query_entry)

    #TODO: add status to the query entry

    try:
        db.commit()
        db.refresh(query_entry)
    except Exception as e:
        print(f"Error committing query entry to db: {e}")
        db.rollback()
    
    return {
        "message": "Query added successfully",
        "status": "success"
    }
    # Implementation goes here
    pass

@router.get("/list")
def list_queries(db:Session=Depends(get_db)):
    """
    List all queries in the database.
    """
    query_entries: List[QueryEntry] = db.query(QueryEntry).all()

    output = []

    for query_entry in query_entries:
        # get the processor from db
        for processor in query_entry.processor_node_ids:
            processor_node: ProcessorNode = db.query(ProcessorNode).filter(ProcessorNode.id == processor).first()
            if not processor_node:
                continue

            queries: List[Query] = query_entry.queries
            query_responses = []

            for query in queries:
                print(f"Query: {query}")
                # exit(0)
                processor_info_endpoint = f"http://{processor_node.serving_host}:{processor_node.serving_port}/info/{query['name']}"

                response = requests.get(processor_info_endpoint)

                if response.status_code != 200:
                    print(f"Error getting processor node info: {response.text}")
                    continue

                processor_info = response.json()
                query_responses.append(processor_info)

            output.append(
                {
                    "entry_endpoints": query_entry.entry_endpoints,
                    "queries_states": query_responses
                }
            )

    return output