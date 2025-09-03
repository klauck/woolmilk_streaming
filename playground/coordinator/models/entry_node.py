from sqlalchemy import Column, Integer, String, JSON
from database import Base

class EntryNode(Base):
    __tablename__ = "entry_nodes"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, index=True)
    ssh_host = Column(String)
    ssh_port = Column(Integer)
    ssh_user = Column(String)
    ssh_password = Column(String)
    serving_host = Column(String)
    serving_port = Column(Integer)
    parquet_files = Column(JSON, nullable=True)
    env_name = Column(String)  # New column for environment name
    status = Column(String, default="stopped")       # e.g. 'running','stopped','failed'
    status_message = Column(String, default="")
