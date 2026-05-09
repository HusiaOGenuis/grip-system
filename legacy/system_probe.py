import requests

BASE = "http://127.0.0.1:8000"

def test(endpoint):
    print(f"\n🔍 Testing {endpoint}")
    try:
        r = requests.get(BASE + endpoint)
        print("Status:", r.status_code)
        print("Response:", r.text[:200])
    except Exception as e:
        print("❌ Failed:", e)

print("\n🧪 LOCAL PROBE STARTED\n")

test("/")
test("/health")
test("/reports")

print("\n✅ PROBE COMPLETE\n")
