# narrative.py
from typing import Dict, Any


def compose_narrative(
    analysis: Dict[str, Any],
    decision: Dict[str, Any],
) -> Dict[str, Any]:

    column_count = len(analysis["profile"])
    issue_count = len(decision["issues"])

    if decision["verdict"] == "RELIABLE":
        summary = (
            f"Dataset contains {column_count} columns and "
            "shows no significant quality issues."
        )
    else:
        summary = (
            f"Dataset contains {column_count} columns with "
            f"{issue_count} issues impacting reliability."
        )

    actions = [i["issue"] for i in decision["issues"]]

    return {
        "summary": summary,
        "verdict": decision["verdict"],
        "risk_level": decision["risk_level"],
        "recommended_actions": actions,
    }