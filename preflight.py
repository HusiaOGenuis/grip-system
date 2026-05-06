import os
import sys
import platform
import requests
from urllib.parse import urlparse

REQUIRED_ENV = [
    "SUPABASE_URL",
    "SUPABASE_SERVICE_ROLE_KEY",
    "SUPABASE_BUCKET",
]

TIMEOUT = 5


def fail(msg: str):
    print(f"\n[FAIL] {msg}")
    sys.exit(1)


def ok(msg: str):
    print(f"[OK] {msg}")


def run_preflight():
    # 1. Python
    if sys.version_info < (3, 10):
        fail("Python >= 3.10 is required")
    ok(f"Python {platform.python_version()}")

    # 2. ENV
    missing = [k for k in REQUIRED_ENV if not os.getenv(k)]
    if missing:
        fail(f"Missing env vars: {missing}")

    SUPABASE_URL = os.getenv("SUPABASE_URL").rstrip("/")
    KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    BUCKET = os.getenv("SUPABASE_BUCKET")

    ok("Environment variables present")

    # 3. URL sanity
    parsed = urlparse(SUPABASE_URL)
    if parsed.scheme != "https":
        fail("SUPABASE_URL must be https")
    if not parsed.netloc.endswith(".supabase.co"):
        fail("Invalid Supabase URL")
    ok("SUPABASE_URL valid")

    # 4. Headers (critical fix)
    headers = {
        "Authorization": f"Bearer {KEY}",
        "apikey": KEY,
        "Content-Type": "application/json",
    }

    # 5. STORAGE probe (stronger than auth probe)
    bucket_url = f"{SUPABASE_URL}/storage/v1/bucket/{BUCKET}"
    r = requests.get(bucket_url, headers=headers, timeout=TIMEOUT)

    if r.status_code == 404:
        fail(f"Bucket '{BUCKET}' does not exist")

    if r.status_code != 200:
        fail(f"Storage probe failed: {r.status_code} {r.text}")

    ok("Storage access confirmed")

    # 6. SIGN test
    sign_url = f"{SUPABASE_URL}/storage/v1/object/sign/{BUCKET}/__preflight.txt"
    r = requests.post(sign_url, json={"expiresIn": 30}, headers=headers, timeout=TIMEOUT)

    if r.status_code != 200:
        fail(f"Signed URL failed: {r.status_code} {r.text}")

    ok("Signed URL works")

    print("\n🚀 PRE-FLIGHT PASSED\n")