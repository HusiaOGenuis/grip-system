from typing import Dict, Any


def evaluate_policy(
    *,
    identity: Dict[str, Any],
    action: str,
    context: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Pure GRIP intelligence logic.
    No FastAPI, no auth, no I/O.
    """

    # Service identities always allowed
    if identity.get("role") == "service":
        return {
            "allow": True,
            "reason": "Service identity trusted",
        }

    # Authenticated humans must provide score
    if identity.get("role") == "authenticated":
        if "score" not in context:
            return {
                "allow": False,
                "reason": "Missing required score",
            }

    return {
        "allow": True,
        "reason": "GRIP policy satisfied",
    }