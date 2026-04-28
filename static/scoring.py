def grip_score(data):
    total = len(data)
    if total == 0:
        return {"score": 0, "risk": "INVALID"}

    missing = 0
    duplicates = 0
    seen = set()

    for row in data:
        # missing values
        for v in row.values():
            if v in (None, "", "null"):
                missing += 1

        # duplicate detection
        key = tuple(row.items())
        if key in seen:
            duplicates += 1
        else:
            seen.add(key)

    completeness = 1 - (missing / (total * len(data[0])))
    uniqueness = 1 - (duplicates / total)

    score = (completeness * 0.6 + uniqueness * 0.4) * 100

    if score < 50:
        risk = "HIGH"
    elif score < 75:
        risk = "MEDIUM"
    else:
        risk = "LOW"

    return {
        "completeness": round(completeness * 100, 2),
        "uniqueness": round(uniqueness * 100, 2),
        "score": round(score, 2),
        "risk": risk
    }