from typing import Dict, Any


class ConfidenceOverrideNotPermitted(Exception):
    """Raised when override violates confidence-based restrictions."""


def enforce_confidence_override(
    *,
    confidence: float,
    override_verdict: str,
    attestation: Dict[str, Any],
    policy: Dict[str, Any],
) -> None:
    role = attestation.get("role")
    if not role:
        raise ConfidenceOverrideNotPermitted("Attestation role required")

    for band_name, band in policy["bands"].items():
        if confidence >= band["min_confidence"]:
            allowed_roles = band["allowed_roles"]
            allowed_to = band["allowed_to"]

            if role not in allowed_roles:
                raise ConfidenceOverrideNotPermitted(
                    f"Role {role} may not override {band_name}-confidence decisions"
                )

            if override_verdict not in allowed_to:
                raise ConfidenceOverrideNotPermitted(
                    f"{band_name}-confidence decisions may not be overridden to {override_verdict}"
                )

            return  # allowed

    raise ConfidenceOverrideNotPermitted("No matching confidence band found")
