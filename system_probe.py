import requests

BASE = "http://127.0.0.1:8000"

print("\n🔍 SYSTEM PROBE\n")

endpoints = [
    "/health",
    "/files",
]

for ep in endpoints:
    try:
        r = requests.get(BASE + ep)
        print(f"{ep} -> {r.status_code}")
        print(r.text[:200], "\n")
    except Exception as e:
        print(f"{ep} FAILED -> {e}")

print("DONE\n")