import json
from pathlib import Path
from datetime import datetime
from typing import Dict, Any
from uuid import uuid4

OVERRIDE_DIR = Path(__file__).resolve().parents[1] / "records" / "overrides"
OVERRIDE_DIR.mkdir(parents=True, exist_ok=True)


def write_override_record(
    *,
    trace_id: str,
    original_verdict: str,
    override_verdict: str,
    attestation: Dict[str, Any],
    config_snapshot: Dict[str, Any],
    engine_version: str,
) -> str:
    """
    Persist an override decision.
    Overrides are strictly trace-bound to an original decision.
    """

    override_id = str(uuid4())
    timestamp = datetime.utcnow().isoformat() + "Z"

    record = {
        "override_id": override_id,
        "trace_id": trace_id,
        "timestamp": timestamp,
        "engine_version": engine_version,
        "original_verdict": original_verdict,
        "override_verdict": override_verdict,
        "attestation": attestation,
        "config_snapshot": config_snapshot,
    }

    path = OVERRIDE_DIR / f"{override_id}.json"
    if path.exists():
        raise RuntimeError(f"Override record already exists: {override_id}")

    with open(path, "w", encoding="utf-8") as f:
        json.dump(record, f, indent=2)

    return override_id