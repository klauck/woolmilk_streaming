import json
from typing import Optional


def decode_batch_metadata(meta) -> dict:
    if meta is None:
        return {"id": None, "wm": None}
    raw = bytes(meta).decode("utf-8")
    try:
        d = json.loads(raw)
        if isinstance(d, dict):
            return {"id": d.get("id"), "wm": d.get("wm")}
    except json.JSONDecodeError:
        pass
    return {"id": raw, "wm": None}


def encode_batch_metadata(batch_id: Optional[str], watermark: Optional[int] = None) -> bytes:
    payload = {"id": batch_id}
    if watermark is not None:
        payload["wm"] = watermark
    return json.dumps(payload).encode("utf-8")
