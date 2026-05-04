def interpret(escalation, decision):
    return (
        f"Dataset assessed as {escalation['level']} risk. "
        f"Key risk classes detected: {', '.join(escalation['classes'])}. "
        f"System verdict: {decision['verdict']}. "
        f"Priority: {decision['priority']}. "
        f"Recommended next step: {', '.join(escalation['recommended_actions'])}."
    )
