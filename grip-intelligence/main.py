from fastapi import FastAPI, HTTPException
from typing import Dict, Any

from policies import evaluate_policy

app = FastAPI(title="GRIP Intelligence Service")


@app.post("/evaluate")
def evaluate(payload: Dict[str, Any]):
    try:
        return evaluate_policy(
            identity=payload["identity"],
            action=payload["action"],
            context=payload["context"],
        )
    except KeyError as e:
        raise HTTPException(
            status_code=400,
            detail=f"Missing required field: {str(e)}",
        )