def detect_signals(data):
    signals = []

    for row in data:

        # NON NUMERIC AMOUNT
        try:
            float(row.get("amount", 0))
        except:
            signals.append("NON_NUMERIC_AMOUNT")

        # INVALID DATE
        if not row.get("date"):
            signals.append("INVALID_DATE")

        # SUSPICIOUS VENDOR
        vendor = str(row.get("vendor_ref", "")).lower()
        if "test" in vendor or "unknown" in vendor:
            signals.append("SUSPICIOUS_VENDOR")

    # DUPLICATES
    seen = set()
    for row in data:
        tid = row.get("transaction_id")
        if tid in seen:
            signals.append("DUPLICATE_TRANSACTION")
        seen.add(tid)

    return list(set(signals))