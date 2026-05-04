SIGNAL_SEVERITY = {
    "MISSING_CRITICAL_FIELD": {
        "weight": 5,
        "class": "DATA_INTEGRITY",
        "default_action": "BLOCK"
    },
    "DUPLICATE_RECORD": {
        "weight": 2,
        "class": "QUALITY",
        "default_action": "REVIEW"
    },
    "ANOMALOUS_VALUE": {
        "weight": 3,
        "class": "RISK",
        "default_action": "ESCALATE"
    },
    "INVALID_DATE": {
        "weight": 3,
        "class": "DATA_INTEGRITY",
        "default_action": "ESCALATE"
    },
    "NON_NUMERIC_AMOUNT": {
        "weight": 4,
        "class": "FINANCIAL_RISK",
        "default_action": "ESCALATE"
    },
    "SUSPICIOUS_VENDOR": {
        "weight": 5,
        "class": "FRAUD",
        "default_action": "BLOCK"
    },
    "DUPLICATE_TRANSACTION": {
        "weight": 2,
        "class": "QUALITY",
        "default_action": "REVIEW"
    }
}