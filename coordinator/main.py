import uvicorn
from fastapi import FastAPI

from database import Base, engine
from routers.entry_node import router as entry_node_router
from routers.processor_node import router as processor_node_router

Base.metadata.create_all(bind=engine)

app = FastAPI(title="Coordinator API", description="Manage Processor Nodes")

app.include_router(entry_node_router, prefix="/api", tags=["entry_node"])
app.include_router(processor_node_router, prefix="/api", tags=["processor_node"])

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
