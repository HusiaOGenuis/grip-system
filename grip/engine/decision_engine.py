from grip.engine.decision_result import DecisionResult


class DecisionPause(Exception):
    """Raised when a decision must be escalated."""


def make_decision(*, request: dict, config: dict) -> DecisionResult:
    """
    Pure decision logic. No confidence, no envelope, no persistence.
    """

    score = request.get("score", 0)

    if score >= 80:
        verdict = "ALLOW"
        rationale = "Score exceeds approval threshold"
        remediation = None

    elif score >= 60:
        verdict = "CONDITIONAL"
        rationale = "Score requires conditions"
        remediation = {"action": "request_additional_docs"}

    elif score < 40:
        raise DecisionPause("Score below minimum safe threshold")

    else:
        verdict = "DISALLOW"
        rationale = "Score below approval threshold"
        remediation = {"action": "manual_review"}

    return DecisionResult.new(
        verdict=verdict,
        rationale=rationale,
        remediation=remediation,
    )