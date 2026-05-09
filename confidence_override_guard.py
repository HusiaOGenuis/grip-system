from typing import Dict, Any, Tuple


class ConfidenceOverrideNotPermitted(Exception):
    """Raised when override violates confidence-based restrictions."""


def _select_confidence_band(
    confidence: float,
    bands: Dict[str, Dict[str, Any]],
) -> Tuple[str, Dict[str, Any]]:
    """
    Select the highest applicable confidence band deterministically.
    """
    sorted_bands = sorted(
        bands.items(),
        key=lambda i: i[1]["min_confidence"],
        reverse=True,
    )

    for name, band in sorted_bands:
        if confidence >= band["min_confidence"]:
            return name, band

    raise ConfidenceOverrideNotPermitted("No matching confidence band found")


def enforce_confidence_override(
    *,
    confidence: float,
    envelope: Dict[str, Any],
    override_verdict: str,
    attestation: Dict[str, Any],
    policy: Dict[str, Any],
) -> None:
    """
    Enforce confidence-aware override rules.
    Acts as a mandatory choke-point before override persistence.
    """

    role = attestation.get("role")
    if not role:
        raise ConfidenceOverrideNotPermitted("Attestation role required")

    # 1. Determine applicable confidence band deterministically
    band_name, band = _select_confidence_band(confidence, policy["bands"])

    # 2. Role restriction
    if role not in band["allowed_roles"]:
        raise ConfidenceOverrideNotPermitted(
            f"Role {role} may not override {band_name}-confidence decisions"
        )

    # 3. Verdict restriction
    if override_verdict not in band["allowed_to"]:
        raise ConfidenceOverrideNotPermitted(
            f"{band_name}-confidence decisions may not be overridden to {override_verdict}"
        )

    # 4. Envelope / reliance constraint
    constraints = policy.get("constraints", {})

    if constraints.get("forbid_lowering_confidence_band", False):
        reliance = envelope.get("reliance")

        if reliance == "AUTOMATED" and override_verdict == "ALLOW":
            raise ConfidenceOverrideNotPermitted(
                "AUTOMATED decisions may not be directly overridden to ALLOW"
            )

    return  # explicitly permitted