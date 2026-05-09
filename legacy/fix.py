# fix.py
from typing import Dict, Any


def propose_fixes(decision: Dict[str, Any]) -> Dict[str, Any]:
    fixes = []

    for issue in decision["issues"]:
        if issue["issue"] == "High missing values":
            fixes.append("Investigate or impute missing values")
        if "Numeric" in issue["issue"]:
            fixes.append("Convert column to numeric and clean values")

    return {
        "fix_plan": fixes
    }