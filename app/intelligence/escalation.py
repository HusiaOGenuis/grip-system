import code

from app.intelligence.signal_registry import SIGNAL_SEVERITY

def escalate(signals, score):
    risk_points = 0
    actions = set()
    classes = set()
        
    for s in signals:
        code = s if isinstance(s, str) else s.get("code")
        meta = SIGNAL_SEVERITY.get(code, {})
        risk_points += meta.get("weight", 1)
        actions.add(meta.get("default_action", "IGNORE"))
        classes.add(meta.get("class", "UNKNOWN"))

    if score.get("risk") == "HIGH":
        risk_points += 5
    elif score.get("risk") == "MEDIUM":
        risk_points += 2

    if risk_points >= 10:
        level = "CRITICAL"
    elif risk_points >= 6:
        level = "HIGH"
    elif risk_points >= 3:
        level = "MEDIUM"
    else:
        level = "LOW"

    return {
        "risk_points": risk_points,
        "level": level,
        "classes": list(classes),
        "recommended_actions": list(actions)
    }
