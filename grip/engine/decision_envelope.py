def build_envelope(confidence: float) -> dict:
    if confidence >= 0.8:
        band = "HIGH"
        reliance = "AUTOMATED"
    elif confidence >= 0.6:
        band = "MEDIUM"
        reliance = "HUMAN_REVIEW"
    else:
        band = "LOW"
        reliance = "ESCALATE"

    return {
        "confidence_band": band,
        "reliance": reliance,
        "scope": "CASE_SPECIFIC",
    }