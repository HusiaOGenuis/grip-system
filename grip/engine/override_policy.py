class OverrideNotPermitted(Exception):
    """Raised when a policy-based override is not permitted."""


def validate_override(
    *,
    original_verdict: str,
    override_verdict: str,
    attestation: dict,
    policy: dict,
) -> None:
    """
    Validate override against static override policy.
    """

    role = attestation.get("role")
    if not role:
        raise OverrideNotPermitted("Override attestation must include role")

    allowed = policy.get(original_verdict, [])
    if override_verdict not in allowed:
        raise OverrideNotPermitted(
            f"Override from {original_verdict} to {override_verdict} is not permitted"
        )