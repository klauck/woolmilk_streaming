from typing import Optional


class StreamProvider():
    def __init__(self):
        pass
    
    @staticmethod
    def get_stream_provider(event: str, tuples_per_batch: int, overall_tuples: int, 
                          executable: Optional[str] = None):
        try:
            stream_provider, stream_type = event.split(".")
        except ValueError:
            raise ValueError(f"Invalid event format '{event}'. Expected 'provider.stream_type'")

        if stream_provider == "nexmark":
            from .nexmark_provider import NexmarkProvider
            return NexmarkProvider(stream_type, tuples_per_batch, overall_tuples, executable)
        elif stream_provider == "custom":
            from .custom_provider import CustomProvider
            return CustomProvider(stream_type, tuples_per_batch, overall_tuples)
        else:
            raise ValueError(f"Unsupported stream provider '{stream_provider}'. Supported: 'nexmark', 'custom'")
