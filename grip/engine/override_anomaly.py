import json
from pathlib import Path
from collections import Counter
from typing import Dict, Any, List


RECORDS_DIR = Path(__file__).resolve().parents[1] / "records"
DECISION_DIR = RECORDS_DIR / "decision"
OVERRIDE_DIR = RECORDS_DIR / "override"

# Governance thresholds (tune later, config-driven in future)
ROLE_OVERRIDE_RATE_THRESHOLD = 0.4
VERDICT_OVERRIDE_RATE_THRESHOLD = 0.3


def _load_json(path: Path) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def detect_override_anomalies() -> Dict[str, Any]:
    """
    Detect governance anomalies in override behavior.
    """

    decisions = {}
    for path in DECISION_DIR.glob("*.json"):
        record = _load_json(path)
        decisions[record["decision_id"]] = record

    overrides = []
    for path in OVERRIDE_DIR.glob("*.json"):
        overrides.append(_load_json(path))

    total_decisions = len(decisions)
    total_overrides = len(overrides)

    anomalies: List[Dict[str, Any]] = []

    # --- A. Role-based concentration ---
    overrides_by_role = Counter()
    for o in overrides:
        overrides_by_role[o["attestation"]["role"]] += 1

    for role, count in overrides_by_role.items():
        rate = count / total_overrides if total_overrides > 0 else 0
        if rate > ROLE_OVERRIDE_RATE_THRESHOLD:
            anomalies.append({
                "type": "ROLE_CONCENTRATION",
                "role": role,
                "override_share": round(rate, 3),
                "threshold": ROLE_OVERRIDE_RATE_THRESHOLD,
                "description": (
                    f"Role {role} accounts for a high share of overrides"
                ),
            })

    # --- B. Verdict-specific override spikes ---
    verdict_counts = Counter()
    verdict_overrides = Counter()

    for d in decisions.values():
        verdict_counts[d["result"]["verdict"]] += 1

    for o in overrides:
        verdict_overrides[o["original_verdict"]] += 1

    for verdict, overridden in verdict_overrides.items():
        base = verdict_counts.get(verdict, 0)
        if base == 0:
            continue

        rate = overridden / base
        if rate > VERDICT_OVERRIDE_RATE_THRESHOLD:
            anomalies.append({
                "type": "VERDICT_SPIKE",
                "verdict": verdict,
                "override_rate": round(rate, 3),
                "threshold": VERDICT_OVERRIDE_RATE_THRESHOLD,
                "description": (
                    f"Verdict {verdict} is overridden unusually often"
                ),
            })

    # --- C. Directional drift ---
    direction_counter = Counter()
    for o in overrides:
        if o["original_verdict"] != o["override_verdict"]:
            direction = f"{o['original_verdict']}→{o['override_verdict']}"
            direction_counter[direction] += 1

    if direction_counter:
        most_common, count = direction_counter.most_common(1)[0]
        drift_rate = count / total_overrides if total_overrides > 0 else 0

        if drift_rate > 0.5:
            anomalies.append({
                "type": "DIRECTIONAL_DRIFT",
                "direction": most_common,
                "share": round(drift_rate, 3),
                "description": (
                    f"Overrides predominantly move in direction {most_common}"
                ),
            })

    return {
        "summary": {
            "total_decisions": total_decisions,
            "total_overrides": total_overrides,
            "anomalies_detected": len(anomalies),
        },
        "anomalies": anomalies,
    }