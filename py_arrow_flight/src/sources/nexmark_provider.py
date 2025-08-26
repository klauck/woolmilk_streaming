from typing import Iterator, Dict
import pyarrow as pa
from .stream_provider import StreamProvider
from .data_generator import NexmarkDataGenerator
    
class NexmarkProvider(StreamProvider):
    """
    Stream provider for Nexmark data.
    This class generates data for the specified Nexmark event type.
    """
    def __init__(self, stream_type: str, tuples_per_batch: int, overall_tuples: int, executable: str):
        super().__init__()
        self.stream_type = stream_type
        self.tuples_per_batch = tuples_per_batch
        self.overall_tuples = overall_tuples
        self.data_generator = NexmarkDataGenerator(
            event_type=stream_type,
            chunk_size=tuples_per_batch,
            no_records=overall_tuples,
            executable=executable
        )
        
    def get_stream(self) -> Iterator[Dict[str, pa.Table]]:
        """Get the Nexmark stream data as dictionary."""
        
        for person_tbl, auction_tbl, bid_tbl, category_tbl in self.data_generator.generate():
            tables_dict = {}
            
            if person_tbl is not None:
                tables_dict["person"] = person_tbl
            if auction_tbl is not None:
                tables_dict["auction"] = auction_tbl
            if bid_tbl is not None:
                tables_dict["bid"] = bid_tbl
            if category_tbl is not None:
                tables_dict["category"] = category_tbl
            
            
            yield tables_dict
    
    def __str__(self) -> str:
        return f"NexmarkProvider(stream_type={self.stream_type}, tuples_per_batch={self.tuples_per_batch}, overall_tuples={self.overall_tuples})"
