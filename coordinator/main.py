import asyncio
import socket

import uvicorn
from fastapi import FastAPI

from database import Base, engine, SessionLocal
from routers.entry_node import router as entry_node_router
from routers.processor_node import router as processor_node_router
from services.entry_node import list_entry_nodes  # Assumes this returns a list of node models

# Create all tables if they do not exist.
Base.metadata.create_all(bind=engine)

app = FastAPI(title="Coordinator API", description="Manage Processor Nodes")

app.include_router(entry_node_router, prefix="/api", tags=["entry_node"])
app.include_router(processor_node_router, prefix="/api", tags=["processor_node"])


def update_all_node_status():
    db = SessionLocal()
    try:
        nodes = list_entry_nodes(db)
        for node in nodes:
            host_to_check = node.ssh_host
            port_to_check = node.serving_port
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(2.0)
            try:
                s.connect((host_to_check, port_to_check))
                s.close()
                node.status = "running"
                node.status_message = f"Port {port_to_check} on {host_to_check} is open. Node is working."
                print(f"Port {port_to_check} on {host_to_check} is open. Node is working.")
            except Exception as e:
                node.status = "stopped"
                node.status_message = f"Port {port_to_check} on {host_to_check} is closed or unreachable. Error: {e}"
                print(f"Port {port_to_check} on {host_to_check} is closed or unreachable. Error: {e}")
        db.commit()
    except Exception as exc:
        print(f"Error while updating node statuses: {exc}")
    finally:
        db.close()


async def periodic_node_status_check():
    while True:
        await asyncio.to_thread(update_all_node_status)
        await asyncio.sleep(5*60)  # Check every 5 minutes


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(periodic_node_status_check())


@app.get("/")
async def root():
    return {"message": "Coordinator API is running. Background node status check is active."}


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
