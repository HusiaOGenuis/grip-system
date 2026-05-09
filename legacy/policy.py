# policy.py
from typing import Dict, Any


def assess_risk(analysis: Dict[str, Any]) -> Dict[str, Any]:
    issues = []
    high_risk = False

    for col, meta in analysis["profile"].items():
        if meta["null_ratio"] > 0.3:
            issues.append({
                "column": col,
                "issue": "High missing values",
                "severity": "medium",
            })

        if meta["type"] == "numeric" and meta["dtype"] == "object":
            issues.append({
                "column": col,
                "issue": "Numeric data stored as text",
                "severity": "high",
            })
            high_risk = True

    verdict = "RELIABLE"
    risk_level = "LOW"

    if issues:
        verdict = "UNRELIABLE"
        risk_level = "HIGH" if high_risk else "MEDIUM"

    return {
        "verdict": verdict,
        "risk_level": risk_level,
        "issues": issues,
    }