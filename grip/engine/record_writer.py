import json
from pathlib import Path
from datetime import datetime
from typing import Dict, Any


RECORD_DIR = Path(__file__).resolve().parents[1] / "records" / "decisions"
RECORD_DIR.mkdir(parents=True, exist_ok=True)


def write_decision_record(
    *,
    request: Dict[str, Any],
    result: Dict[str, Any],
    config_snapshot: Dict[str, Any],
    engine_version: str,
) -> str:
    """
    Persist a decision record. Trace ID is authoritative.
    """

    trace_id = result["trace_id"]
    timestamp = datetime.utcnow().isoformat() + "Z"

    record = {
        "trace_id": trace_id,
        "timestamp": timestamp,
        "engine_version": engine_version,
        "request": request,
        "result": result,
        "config_snapshot": config_snapshot,
    }

    path = RECORD_DIR / f"{trace_id}.json"
    if path.exists():
        raise RuntimeError(f"Decision record already exists: {trace_id}")

    with open(path, "w", encoding="utf-8") as f:
        json.dump(record, f, indent=2)

    return trace_id