from dataclasses import dataclass, asdict
from typing import Optional, Dict, Any
from uuid import uuid4


@dataclass(frozen=True)
class DecisionResult:
    """
    Canonical, immutable representation of a decision outcome.
    This object anchors traceability across the entire GRIP lifecycle.
    """

    trace_id: str
    verdict: str
    rationale: str
    remediation: Optional[Dict[str, Any]] = None

    # Governance-derived attributes
    confidence: Optional[float] = None
    envelope: Optional[Dict[str, Any]] = None

    @staticmethod
    def new(
        *,
        verdict: str,
        rationale: str,
        remediation: Optional[Dict[str, Any]] = None,
    ) -> "DecisionResult":
        """
        Factory method: creates a new decision with a trace ID.
        """
        return DecisionResult(
            trace_id=str(uuid4()),
            verdict=verdict,
            rationale=rationale,
            remediation=remediation,
        )

    def to_record(self) -> Dict[str, Any]:
        """
        Canonical persistence representation.
        """
        return asdict(self)