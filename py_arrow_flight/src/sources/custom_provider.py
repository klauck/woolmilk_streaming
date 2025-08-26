from typing import Iterator, Dict
import pyarrow as pa
import random
from .stream_provider import StreamProvider

class CustomProvider(StreamProvider):
    def __init__(self, stream_type: str, tuples_per_batch: int, overall_tuples: int):
        super().__init__()
        
        if stream_type != "random":
            raise ValueError(f"Invalid stream type '{stream_type}'. CustomProvider only supports 'random'")
            
        self.stream_type = stream_type
        self.tuples_per_batch = tuples_per_batch
        self.overall_tuples = overall_tuples
        
        random.seed(42)
    
    def _generate_random_numbers_batch(self, batch_size: int) -> pa.Table:
        """Generate a batch of random numbers with 'number' column."""
        numbers = [random.randint(1, 100000) for _ in range(batch_size)]
        return pa.table({
            'number': numbers
        })
    
    def get_stream(self) -> Iterator[Dict[str, pa.Table]]:
        """Generate stream of random numbers as dictionary."""
        
        generated_tuples = 0
        batch_count = 0
        
        while generated_tuples < self.overall_tuples:
            batch_count += 1

            remaining_tuples = min(self.tuples_per_batch, self.overall_tuples - generated_tuples)
            
            numbers_tbl = self._generate_random_numbers_batch(remaining_tuples)
            
            generated_tuples += remaining_tuples
            
            yield {"numbers": numbers_tbl}