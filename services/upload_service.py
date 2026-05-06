import requests
from core.supabase_rest_client import SUPABASE_URL, SUPABASE_KEY, SUPABASE_BUCKET


def handle_upload(contents: bytes, filename: str, user_id: str):
    path = f"{user_id}/{filename}"

    url = f"{SUPABASE_URL}/storage/v1/object/upload/sign/{object_path}"
    headers = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "text/csv",
    "x-upsert": "true",
}

    response = requests.post(url, headers=headers, data=contents)

    if response.status_code not in [200, 201]:
        return {
            "status": "error",
            "code": response.status_code,
            "response": response.text,
        }

    return {
        "status": "success",
        "path": path,
    }
