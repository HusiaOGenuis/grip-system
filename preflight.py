# preflight.py
import os
import sys
import json
import platform
import requests
from urllib.parse import urlparse
from pathlib import Path
from datetime import datetime

POLICY_VERSION = "2.0.0"
TIMEOUT = 5

REQUIRED_ENV = [
    "SUPABASE_URL",
    "SUPABASE_SERVICE_ROLE_KEY",
    "SUPABASE_BUCKET",
]

DIAGNOSTICS = []


def record(level, message, file=None, line=None, fix=None):
    DIAGNOSTICS.append({
        "level": level,
        "message": message,
        "file": file,
        "line": line,
        "fix": fix,
    })


def fail(message, **kw):
    record("FAIL", message, **kw)
    raise SystemExit(1)


def warn(message, **kw):
    record("WARN", message, **kw)


def ok(msg):
    print(f"[OK] {msg}")


# ---------------- Runtime ----------------

def check_python():
    if sys.version_info < (3, 10):
        fail("Python >= 3.10 required")
    ok(f"Python {platform.python_version()}")


# ---------------- Environment ----------------

def check_env():
    missing = [k for k in REQUIRED_ENV if not os.getenv(k)]
    if missing:
        fail(f"Missing env vars: {missing}")

    url = os.getenv("SUPABASE_URL").rstrip("/")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    bucket = os.getenv("SUPABASE_BUCKET")

    ok("Environment variables present")
    return url, key, bucket


def check_url(url):
    parsed = urlparse(url)
    if parsed.scheme != "https":
        fail("SUPABASE_URL must be https")
    if not parsed.netloc.endswith(".supabase.co"):
        fail("Invalid Supabase project URL")
    ok("SUPABASE_URL validated")


# ---------------- Supabase ----------------

def headers(key):
    return {
        "Authorization": f"Bearer {key}",
        "apikey": key,
        "Content-Type": "application/json",
    }


def check_storage(url, bucket, key):
    r = requests.get(
        f"{url}/storage/v1/bucket/{bucket}",
        headers=headers(key),
        timeout=TIMEOUT,
    )
    if r.status_code != 200:
        fail("Storage bucket inaccessible", fix="Check bucket and service role key")
    ok("Storage bucket accessible")


def check_upload_signing(url, bucket, key):
    test_path = f"{bucket}/__preflight_test__.csv"
    r = requests.post(
        f"{url}/storage/v1/object/upload/sign/{test_path}",
        json={"expiresIn": 30},
        headers=headers(key),
        timeout=TIMEOUT,
    )
    if r.status_code != 200:
        fail("Upload signing failed", fix="Check Supabase permissions")
    ok("Upload signing capability verified")


# ---------------- Code Contracts ----------------

def check_code():
    main = Path("main.py")
    analysis = Path("analysis.py")

    if not main.exists():
        fail("main.py missing")

    content = main.read_text()

    if "/object/sign/" in content:
        fail("Forbidden download signing endpoint detected", file="main.py")

    if "/object/upload/sign/" not in content:
        fail("Upload signing endpoint missing", file="main.py")

    if not analysis.exists():
        warn("analysis.py missing; /analyze endpoint may fail")

    ok("Backend code contracts validated")


# ---------------- Reports ----------------

def emit_reports():
    ts = datetime.utcnow().isoformat() + "Z"

    Path("preflight-report.json").write_text(
        json.dumps({
            "policy_version": POLICY_VERSION,
            "timestamp": ts,
            "results": DIAGNOSTICS,
        }, indent=2)
    )

    Path("preflight-report.sarif").write_text(
        json.dumps({
            "version": "2.1.0",
            "runs": [{
                "tool": {"driver": {"name": "preflight", "version": POLICY_VERSION}},
                "results": [
                    {
                        "level": "error" if d["level"] == "FAIL" else "warning",
                        "message": {"text": d["message"]},
                        "locations": [{
                            "physicalLocation": {
                                "artifactLocation": {"uri": d.get("file", "project")},
                                "region": {"startLine": d.get("line", 1)},
                            }
                        }] if d.get("file") else [],
                    } for d in DIAGNOSTICS
                ],
            }],
        }, indent=2)
    )


def run_preflight():
    try:
        check_python()
        url, key, bucket = check_env()
        check_url(url)
        check_storage(url, bucket, key)
        check_upload_signing(url, bucket, key)
        check_code()
        print("\n✅ PRE-FLIGHT PASSED\n")
    except SystemExit:
        print("\n❌ PRE-FLIGHT FAILED\n")
        emit_reports()
        raise
    emit_reports()


if __name__ == "__main__":
    run_preflight()