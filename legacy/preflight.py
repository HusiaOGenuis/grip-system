"""
PRE-FLIGHT COMPLIANCE GATE
=========================

Enforces:
- Runtime correctness
- Environment integrity
- Supabase contracts
- Code contracts
- Dependency integrity (NEW)

Fails fast. No runtime imports. No side effects.
"""

import os
import sys
import platform
import requests
import ast
import re
from urllib.parse import urlparse
from pathlib import Path

TIMEOUT = 5

# -------------------------
# REQUIRED ENV VARS
# -------------------------
REQUIRED_ENV = [
    "SUPABASE_URL",
    "SUPABASE_SERVICE_ROLE_KEY",
    "SUPABASE_BUCKET",
]

# -------------------------
# DEPENDENCY POLICY
# -------------------------
CORE_DEPENDENCIES = {
    "fastapi": ["fastapi"],
    "uvicorn": ["uvicorn"],
    "requests": ["requests"],
    "pandas": ["pandas"],
}

OPTIONAL_DEPENDENCIES = {
    "opentelemetry": [
        "opentelemetry-api",
        "opentelemetry-sdk",
    ],
}

RUNTIME_FILES = [
    Path("main.py"),
    Path("analysis.py"),
]

# -------------------------
# LOGGING HELPERS
# -------------------------
def fail(msg: str, fix: str | None = None):
    print(f"\n[FAIL] {msg}")
    if fix:
        print(f"       Fix: {fix}")
    sys.exit(1)

def ok(msg: str):
    print(f"[OK] {msg}")

# -------------------------
# BASIC CHECKS
# -------------------------
def check_python():
    if sys.version_info < (3, 10):
        fail("Python >= 3.10 required")
    ok(f"Python {platform.python_version()}")

def check_env():
    missing = [k for k in REQUIRED_ENV if not os.getenv(k)]
    if missing:
        fail(f"Missing env vars: {missing}")

    url = os.getenv("SUPABASE_URL").rstrip("/")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    bucket = os.getenv("SUPABASE_BUCKET")

    ok("Environment variables present")
    return url, key, bucket

def check_url(url: str):
    parsed = urlparse(url)
    if parsed.scheme != "https":
        fail("SUPABASE_URL must use https")
    if not parsed.netloc.endswith(".supabase.co"):
        fail("SUPABASE_URL must be a Supabase project URL")
    ok("SUPABASE_URL validated")

# -------------------------
# SUPABASE CHECKS
# -------------------------
def build_headers(key: str):
    return {
        "Authorization": f"Bearer {key}",
        "apikey": key,
        "Content-Type": "application/json",
    }

def check_storage(url, bucket, headers):
    r = requests.get(
        f"{url}/storage/v1/bucket/{bucket}",
        headers=headers,
        timeout=TIMEOUT,
    )
    if r.status_code != 200:
        fail("Storage bucket inaccessible", "Check bucket name or service role key")
    ok("Storage bucket accessible")

def check_upload_signing(url, bucket, headers):
    test_path = f"{bucket}/__preflight_test__.csv"
    r = requests.post(
        f"{url}/storage/v1/object/upload/sign/{test_path}",
        json={"expiresIn": 30},
        headers=headers,
        timeout=TIMEOUT,
    )
    if r.status_code != 200:
        fail("Upload signing capability missing", "Verify Supabase permissions")
    ok("Upload signing capability verified")

# -------------------------
# CODE CONTRACT CHECK
# -------------------------
def check_code_contracts():
    main = Path("main.py")
    if not main.exists():
        fail("main.py not found")

    content = main.read_text()

    if "/object/sign/" in content:
        fail(
            "Forbidden download signing endpoint detected",
            "Use /storage/v1/object/upload/sign/",
        )

    if "/object/upload/sign/" not in content:
        fail(
            "Upload signing endpoint missing",
            "Add upload signing logic",
        )

    ok("Backend code contracts validated")

# -------------------------
# DEPENDENCY GATE (NEW)
# -------------------------
def parse_requirements():
    req = Path("requirements.txt")
    if not req.exists():
        fail("requirements.txt missing")

    declared = set()
    for line in req.read_text().lower().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        name = re.split(r"[<>=!]", line)[0]
        declared.add(name)

    return declared

def parse_imports(py_file: Path):
    tree = ast.parse(py_file.read_text(), filename=str(py_file))
    imports = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for n in node.names:
                imports.add(n.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                imports.add(node.module.split(".")[0])

    return imports

def check_dependencies():
    declared = parse_requirements()
    used_imports = set()

    for f in RUNTIME_FILES:
        if f.exists():
            used_imports |= parse_imports(f)

    # Core dependencies
    for mod, pkgs in CORE_DEPENDENCIES.items():
        if mod in used_imports and not any(p in declared for p in pkgs):
            fail(
                f"Missing dependency for import '{mod}'",
                f"Add one of {pkgs} to requirements.txt",
            )

    # Optional dependencies
    for mod, pkgs in OPTIONAL_DEPENDENCIES.items():
        if mod in used_imports and not any(p in declared for p in pkgs):
            fail(
                f"Optional dependency '{mod}' used but not declared",
                f"Add one of {pkgs} to requirements.txt or remove import",
            )

    ok("Dependency integrity verified")

# -------------------------
# ENTRY POINT
# -------------------------
def run_preflight():
    check_python()

    url, key, bucket = check_env()
    check_url(url)

    headers = build_headers(key)

    check_storage(url, bucket, headers)
    check_upload_signing(url, bucket, headers)

    check_code_contracts()

    # ✅ Dependency gate is enforced HERE
    check_dependencies()

    print("\n✅ PRE-FLIGHT PASSED\n")

if __name__ == "__main__":
    run_preflight()