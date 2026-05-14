from typing import Dict, Any


class GripDecision:
    def __init__(self, allow: bool, reason: str):
        self.allow = allow
        self.reason = reason


def evaluate_grip_policy(
    *,
    identity: Dict[str, Any],
    action: str,
    context: Dict[str, Any],
) -> GripDecision:
    """
    GRIP intelligence gate.

    Inputs:
    - identity: resolved identity (JWT or API-key)
    - action: semantic action identifier
    - context: request-level context

    Output:
    - allow / deny with explanation
    """

    # ------------------------------------------------------------------
    # RULE 1 — Service identities are always allowed
    # ------------------------------------------------------------------
    if identity.get("role") == "service":
        return GripDecision(
            allow=True,
            reason="Service identity trusted",
        )

    # ------------------------------------------------------------------
    # RULE 2 — Authenticated humans must provide score
    # ------------------------------------------------------------------
    if identity.get("role") == "authenticated":
        if "score" not in context:
            return GripDecision(
                allow=False,
                reason="Missing required score for decision",
            )

    # ------------------------------------------------------------------
    # DEFAULT — Allow
    # ------------------------------------------------------------------
    return GripDecision(
        allow=True,
        reason="GRIP policy satisfied",
    )