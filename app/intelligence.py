def detect_signals(data):
    signals = set()
    seen_ids = set()

    for row in data:

        # INVALID DATE
        date = str(row.get("date"))
        if "INVALID" in date or date.strip() == "":
            signals.add("INVALID_DATE")

        # NON NUMERIC AMOUNT
        try:
            float(row.get("amount"))
        except:
            signals.add("NON_NUMERIC_AMOUNT")

        # DUPLICATES
        tx = row.get("transaction_id")
        if tx in seen_ids:
            signals.add("DUPLICATE_TRANSACTION")
        else:
            seen_ids.add(tx)

        # SUSPICIOUS VENDOR
        vendor = str(row.get("vendor_ref"))
        if any(c in vendor for c in "!@#$%^&*()"):
            signals.add("SUSPICIOUS_VENDOR")

    return list(signals)