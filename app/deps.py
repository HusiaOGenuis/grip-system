from fastapi import Header, HTTPException, Depends
from .db import get_conn

def get_current_user(x_api_key: str = Header(...)):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT api_key FROM users WHERE api_key=%s", (x_api_key,))
            user = cur.fetchone()
            if not user:
                raise HTTPException(status_code=403, detail="INVALID_API_KEY")
            return x_api_key


def get_dataset(dataset_id: str, api_key: str = Depends(get_current_user)):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT parsed_json FROM datasets
                WHERE id=%s AND api_key=%s
            """, (dataset_id, api_key))

            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="DATASET_NOT_FOUND")

            return {"id": dataset_id, "data": row[0]}