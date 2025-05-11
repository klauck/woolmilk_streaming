from datetime import datetime
from typing import List, Optional
from fastapi import FastAPI, BackgroundTasks
from processor_node import ProcessorNode
from api_models import Query, QueryStatus, InfoResponse, QueryAddedResponse, AddQueryRequest, EntryEndpoint

app = FastAPI()

query_queue: List[Query] = [] # pending & running
history: List[QueryStatus] = [] 

@app.get("/info", response_model=InfoResponse)
def get_info():
    running = next((q for q in history if q.status == "running"), None)
    return InfoResponse(
        node_id=app.state.node_id,
        status="running" if running else "idle",
        current_query=running,
        all_queries=history
    )

# create api at info/{query_name}
@app.get("/info/{query_name}")
def get_query_info(query_name: str):
    query = next((q for q in history if q.name == query_name), None)
    if not query:
        return {"message": "Query not found", "status": "error"}
    return query

@app.post("/add-query", response_model=QueryAddedResponse)
def add_query(payload: AddQueryRequest, background_tasks: BackgroundTasks):
    query_queue.extend(payload.queries)

    for q in payload.queries:
        history.append(
            QueryStatus(
                name=q.name,
                query=q.query,
                start_time=None,
                finished_time=None,
                time_taken=None,
                status="pending"
            )
        )

    # if nothing is executing, start the background worker
    if not any(q.status == "running" for q in history):
        background_tasks.add_task(
            _process_queue,
            payload.entry_endpoints,
            app.state.node_id,
            app.state.exit_host,
            app.state.exit_port
        )

    return QueryAddedResponse(
        message="Queries enqueued",
        total_in_queue=len(query_queue)
    )

def _process_queue(entry_eps: List[EntryEndpoint],
                   node_id: str,
                   exit_host: Optional[str],
                   exit_port: Optional[int]):
    while query_queue:
        q = query_queue.pop(0)

        # find its status record in history
        qs = next(h for h in history if h.name == q.name and h.status == "pending")
        qs.status = "running"
        qs.start_time = datetime.utcnow()

        # run the single query through ProcessorNode
        proc = ProcessorNode(node_id, entry_eps, exit_host, exit_port)
        stats = proc.run_query(q)

        qs.finished_time = datetime.utcnow()
        qs.time_taken = (qs.finished_time - qs.start_time).total_seconds()
        qs.status = "finished"
        qs.stats = stats