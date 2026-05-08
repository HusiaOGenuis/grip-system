from fastapi import FastAPI
from fastapi import Header
from analysis import (
    fetch_csv,
    analyze_dataframe
)
from narrative import (
    compose_narrative
)

app = FastAPI()

API_KEY = "your-secret-key"


@app.get("/")
def home():

    return {
        "status": "GripSystem Online"
    }


@app.get("/analyze")
def analyze(
    path: str,
    user_id: str,
    x_api_key: str = Header(default="")
):

    if x_api_key != API_KEY:

        return {
            "status": "error",
            "message": "Unauthorized"
        }

    df = fetch_csv(path)

    analysis = analyze_dataframe(
        df=df,
        user_id=user_id,
        object_path=path
    )

    narrative = compose_narrative(
        analysis
    )

    return {
        "status": "success",
        "analysis": analysis,
        "narrative": narrative
    }
