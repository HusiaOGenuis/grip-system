import os
import sys
from dotenv import load_dotenv
from supabase import create_client, Client

# ==========================================
# LOAD ENV
# ==========================================
load_dotenv()

# ==========================================
# VALIDATE ENVIRONMENT
# ==========================================
def validate_environment():
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_KEY")
    bucket = os.getenv("SUPABASE_BUCKET")

    errors = []

    print("\n[SUPABASE CHECK]\n")

    if not url:
        errors.append("Missing SUPABASE_URL")

    if not key:
        errors.append("Missing SUPABASE_SERVICE_KEY")

    if not bucket:
        errors.append("Missing SUPABASE_BUCKET")

    if errors:
        print("CRITICAL:")
        for e in errors:
            print("-", e)
        sys.exit(1)

    print("Environment OK\n")
    return url, key, bucket


SUPABASE_URL, SUPABASE_KEY, SUPABASE_BUCKET = validate_environment()

# ==========================================
# CREATE CLIENT
# ==========================================
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ==========================================
# TEST STORAGE
# ==========================================
def test_storage():
    print("Testing storage...")
    try:
        supabase.storage.from_(SUPABASE_BUCKET).list(path="")
        print("Storage OK\n")
    except Exception as e:
        print("STORAGE FAILURE:", str(e))
        sys.exit(1)


test_storage()