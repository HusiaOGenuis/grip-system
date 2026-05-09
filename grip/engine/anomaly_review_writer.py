import json
from pathlib import Path
from datetime import datetime
from uuid import uuid4
from typing import Dict, Any

REVIEW_DIR = Path(__file__).resolve().parents[1] / "records" / "anomaly_review"
REVIEW_DIR.mkdir(parents=True, exist_ok=True)


def write_anomaly_review(
    *,
    anomaly_type: str,
    anomaly_reference: Dict[str, Any],
    review: Dict[str, Any],
) -> str:
    review_id = str(uuid4())
    timestamp = datetime.utcnow().isoformat() + "Z"

    record = {
        "review_id": review_id,
        "timestamp": timestamp,
        "anomaly_type": anomaly_type,
        "anomaly_reference": anomaly_reference,
        "review": review,
    }

    path = REVIEW_DIR / f"{review_id}.json"
    if path.exists():
        raise RuntimeError("Anomaly review already exists")

    with open(path, "w", encoding="utf-8") as f:
        json.dump(record, f, indent=2)

    return review_id
