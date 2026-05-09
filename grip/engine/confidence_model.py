from typing import Dict, Any


def compute_confidence(
    *,
    score: int,
    verdict: str,
    request: Dict[str, Any],
) -> float:
    """
    Returns confidence in range [0.0, 1.0]
    """

    confidence = 0.5  # baseline

    # Distance from thresholds
    if verdict == "ALLOW":
        confidence += min((score - 80) / 20, 0.3)
    elif verdict == "DISALLOW":
        confidence += min((50 - score) / 20, 0.3)
    elif verdict == "CONDITIONAL":
        confidence += 0.1

    # Data completeness
    if request.get("provenance"):
        confidence += 0.1

    # Cap bounds
    return round(max(0.0, min(confidence, 1.0)), 3)