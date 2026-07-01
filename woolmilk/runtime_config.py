import json
from dataclasses import asdict, dataclass
from typing import List, Optional

VALID_COMPRESSIONS = {None, "zstd", "lz4"}
VALID_ENCODINGS = {None, "dictionary"}


@dataclass
class RuntimeConfig:
    compression: Optional[str] = None
    encoding: Optional[str] = None
    columns_to_encode: Optional[List[str]] = None
    use_buffering: bool = False
    tuples_per_batch: int = 8192
    query: Optional[str] = None
    query_result_schema: Optional[dict] = None

    def validate(self) -> None:
        if self.compression not in VALID_COMPRESSIONS:
            raise ValueError(f"invalid compression: {self.compression!r}")
        if self.encoding not in VALID_ENCODINGS:
            raise ValueError(f"invalid encoding: {self.encoding!r}")
        if self.encoding == "dictionary" and not self.columns_to_encode:
            raise ValueError(
                "columns_to_encode is required when encoding == 'dictionary'"
            )
        if not isinstance(self.tuples_per_batch, int) or self.tuples_per_batch <= 0:
            raise ValueError(
                f"tuples_per_batch must be positive int, got {self.tuples_per_batch!r}"
            )
        if self.query_result_schema is not None and not isinstance(
            self.query_result_schema, dict
        ):
            raise ValueError("query_result_schema must be a dict (parsed JSON)")

    def to_json(self) -> bytes:
        return json.dumps(asdict(self)).encode("utf-8")

    @classmethod
    def from_dict(cls, data: dict) -> "RuntimeConfig":
        cfg = cls(
            compression=data.get("compression"),
            encoding=data.get("encoding"),
            columns_to_encode=data.get("columns_to_encode"),
            use_buffering=bool(data.get("use_buffering", False)),
            tuples_per_batch=int(data.get("tuples_per_batch", 8192)),
            query=data.get("query"),
            query_result_schema=data.get("query_result_schema"),
        )
        cfg.validate()
        return cfg

    @classmethod
    def from_json(cls, raw: bytes) -> "RuntimeConfig":
        data = json.loads(raw.decode("utf-8"))
        if not isinstance(data, dict):
            raise ValueError("config payload must be a JSON object")
        return cls.from_dict(data)
