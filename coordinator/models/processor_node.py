# models/processor_node.py

from sqlalchemy import Column, Integer, String, JSON
from database import Base

class ProcessorNode(Base):
    __tablename__ = "processor_nodes"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, index=True)
    ssh_host = Column(String)
    ssh_port = Column(Integer)
    ssh_user = Column(String)
    ssh_password = Column(String)

    exit_host = Column(String)
    exit_port = Column(Integer)

    env_name = Column(String)

    entry_endpoints = Column(JSON, nullable=True)
    queries = Column(JSON, nullable=True)

    status = Column(String, default="stopped")  # e.g. 'running', 'stopped', 'failed'
    status_message = Column(String, default="")
    
class ProcessorQueryDef(Base):
    __tablename__ = "processor_query_defs"

    id = Column(Integer, primary_key=True, index=True)
    processor_node_id = Column(Integer)  # Foreign key to ProcessorNode
    entry_endpoints = Column(JSON, nullable=True)
    queries = Column(JSON, nullable=True)
    status = Column(String, default="stopped")  # e.g. 'running', 'stopped', 'failed' , 'completed'