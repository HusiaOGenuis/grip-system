import json
from pathlib import Path
from typing import Dict, Any

DECISION_DIR = Path(__file__).resolve().parents[1] / "records" / "decisions"
OVERRIDE_DIR = Path(__file__).resolve().parents[1] / "records" / "overrides"


def resolve_effective_verdict(trace_id: str) -> Dict[str, Any]:
    """
    Resolve the effective verdict for a trace ID, considering overrides.
    """

    decision_path = DECISION_DIR / f"{trace_id}.json"
    if not decision_path.exists():
        raise RuntimeError(f"No decision found for trace_id: {trace_id}")

    with open(decision_path, "r", encoding="utf-8") as f:
        decision = json.load(f)

    effective_verdict = decision["result"]["verdict"]
    source = "ORIGINAL"

    for override_file in OVERRIDE_DIR.glob("*.json"):
        with open(override_file, "r", encoding="utf-8") as f:
            override = json.load(f)

        if override["trace_id"] == trace_id:
            effective_verdict = override["override_verdict"]
            source = "OVERRIDE"

    return {
        "trace_id": trace_id,
        "effective_verdict": effective_verdict,
        "source": source,
    }