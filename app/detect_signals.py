def detect_signals(data):
    signals = []

    seen_ids = set()

    for i, row in enumerate(data):

        # Missing critical fields
        for key, value in row.items():
            if value in (None, "", "null"):
                signals.append("MISSING_CRITICAL_FIELD")

        # Duplicate detection (simple)
        identifier = tuple(row.items())
        if identifier in seen_ids:
            signals.append("DUPLICATE_TRANSACTION")
        else:
            seen_ids.add(identifier)

        # Non-numeric amount
        if "amount" in row:
            try:
                float(row["amount"])
            except:
                signals.append("NON_NUMERIC_AMOUNT")

        # Invalid date (basic check)
        if "date" in row:
            if not isinstance(row["date"], str) or len(row["date"]) < 6:
                signals.append("INVALID_DATE")

        # Suspicious vendor (example rule)
        if "vendor_ref" in row and row["vendor_ref"] == "UNKNOWN":
            signals.append("SUSPICIOUS_VENDOR")

    return list(set(signals))