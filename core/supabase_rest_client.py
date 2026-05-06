import os
import sys
import requests
from pathlib import Path

# ==========================================
# LOAD .env (no external deps)
# ==========================================
ENV_PATH = Path(__file__).resolve().parent.parent / ".env"

if ENV_PATH.exists():
    for line in ENV_PATH.read_text().splitlines():
        if "=" in line and not line.strip().startswith("#"):
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

print(f"\n[ENV FILE PATH] {ENV_PATH}\n")

# ==========================================
# VALIDATION
# ==========================================
def validate_environment():
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_KEY")
    bucket = os.getenv("SUPABASE_BUCKET")

    errors = []

    print("[ENV DEBUG]")
    print("SUPABASE_URL:", url)
    print("SUPABASE_SERVICE_KEY:", (key[:20] + "...") if key else None)
    print("SUPABASE_BUCKET:", bucket)
    print()

    if not url or not url.startswith("https://"):
        errors.append("Invalid or missing SUPABASE_URL")

    if not key or not key.startswith("sb_secret_"):
        errors.append("SUPABASE_SERVICE_KEY must be sb_secret_*")

    if not bucket:
        errors.append("Missing SUPABASE_BUCKET")

    if errors:
        print("[SUPABASE CHECK]\n")
        print("CRITICAL:")
        for e in errors:
            print("-", e)
        print("\nStartup halted.\n")
        sys.exit(1)

    print("[SUPABASE CHECK]\n")
    print("Environment OK\n")

    return url, key, bucket


SUPABASE_URL, SUPABASE_KEY, SUPABASE_BUCKET = validate_environment()

# ==========================================
# HEALTH CHECK (STORAGE)
# ==========================================
def test_storage_connection():
    print("Testing storage...")

    url = f"{SUPABASE_URL}/storage/v1/bucket"
    headers = {
    "apikey": SUPABASE_KEY,
}

    try:
        r = requests.get(url, headers=headers)
        if r.status_code != 200:
            raise Exception(r.text)

        print("Storage OK\n")

    except Exception as e:
        print("STORAGE FAILURE:")
        print(str(e))
        print("\nStartup halted.\n")
        sys.exit(1)


test_storage_connection()