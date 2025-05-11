from sqlalchemy import Column, Integer, String, JSON
from database import Base

class QueryEntry(Base):
    __tablename__ = "query_entries"

    id = Column(Integer, primary_key=True, index=True)
    entry_endpoints = Column(JSON)
    queries = Column(JSON)
    processor_node_ids = Column(JSON)