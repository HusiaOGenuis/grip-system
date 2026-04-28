def grip_score(data):
    total = len(data)
    missing = 0

    for row in data:
        for v in row.values():
            if v in (None, "", "null"):
                missing += 1

    completeness = 1 - (missing / (total * len(data[0])))

    return {
        "completeness_score": round(completeness * 100, 2),
        "risk_flag": "HIGH" if completeness < 0.7 else "LOW"
    }