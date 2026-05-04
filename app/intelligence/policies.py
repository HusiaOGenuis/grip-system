def decide(escalation):
    level = escalation["level"]

    if level == "CRITICAL":
        return {
            "verdict": "REJECT_DATASET",
            "priority": "IMMEDIATE",
            "notify": True
        }

    if level == "HIGH":
        return {
            "verdict": "QUARANTINE",
            "priority": "HIGH",
            "notify": True
        }

    if level == "MEDIUM":
        return {
            "verdict": "ALLOW_WITH_FLAGS",
            "priority": "NORMAL",
            "notify": False
        }

    return {
        "verdict": "ALLOW",
        "priority": "LOW",
        "notify": False
    }
