import json
from pathlib import Path
from collections import Counter, defaultdict
from typing import Dict, Any


RECORDS_DIR = Path(__file__).resolve().parents[1] / "records"
DECISION_DIR = RECORDS_DIR / "decision"
OVERRIDE_DIR = RECORDS_DIR / "override"


def _load_json(path: Path) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def analyze_overrides() -> Dict[str, Any]:
    """
    Compute override governance metrics.
    """

    decisions = {}
    if DECISION_DIR.exists():
        for path in DECISION_DIR.glob("*.json"):
            record = _load_json(path)
            decisions[record["decision_id"]] = record

    overrides = []
    if OVERRIDE_DIR.exists():
        for path in OVERRIDE_DIR.glob("*.json"):
            overrides.append(_load_json(path))

    total_decisions = len(decisions)
    total_overrides = len(overrides)

    override_rate = (
        total_overrides / total_decisions if total_decisions > 0 else 0.0
    )

    # --- Breakdown by role ---
    overrides_by_role = Counter()
    for o in overrides:
        role = o["attestation"]["role"]
        overrides_by_role[role] += 1

    # --- Breakdown by original verdict ---
    overrides_by_original_verdict = Counter()
    override_direction = Counter()

    for o in overrides:
        original = o["original_verdict"]
        overridden = o["override_verdict"]

        overrides_by_original_verdict[original] += 1

        if overridden != original:
            direction = f"{original}→{overridden}"
            override_direction[direction] += 1

    return {
        "totals": {
            "decisions": total_decisions,
            "overrides": total_overrides,
            "override_rate": round(override_rate, 4),
        },
        "by_role": dict(overrides_by_role),
        "by_original_verdict": dict(overrides_by_original_verdict),
        "directionality": dict(override_direction),
    }