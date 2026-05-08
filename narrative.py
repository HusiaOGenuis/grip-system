def compose_narrative(analysis: dict) -> dict:

    decision = analysis.get("decision", {})
    findings = analysis.get("semantic_findings", [])
    quality = analysis.get("data_quality", {})

    verdict = decision.get("verdict", "UNKNOWN")
    risk = decision.get("risk_level", "UNKNOWN")

    health = (
        quality.get("summary", {})
        .get("health_score", 0)
    )

    strengths = []
    weaknesses = []
    recommendations = []

    if health >= 0.8:
        strengths.append(
            "Dataset structure is considered stable."
        )

    if health < 0.5:
        weaknesses.append(
            "Dataset health score is critically low."
        )

    for item in findings:

        issue = item.get("issue", "")
        severity = item.get("severity", "")
        suggestion = item.get("suggestion", "")

        if severity == "high":
            weaknesses.append(issue)

        if suggestion and suggestion not in recommendations:
            recommendations.append(suggestion)

    summary = (
        f"The dataset was evaluated as "
        f"{verdict} with {risk} risk."
    )

    return {
        "summary": summary,
        "health_score": health,
        "strengths": strengths,
        "weaknesses": weaknesses,
        "recommendations": recommendations
    }
