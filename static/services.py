def analyze_data(data):
    record_count = len(data)

    missing = 0
    for row in data:
        for v in row.values():
            if v in (None, "", "null"):
                missing += 1

    return {
        "record_count": record_count,
        "missing_values": missing,
        "status": "analyzed"
    }