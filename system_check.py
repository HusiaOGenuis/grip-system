import importlib

modules = [
    "app.main",
    "app.scoring",
    "app.intelligence",
    "app.intelligence.escalation",
    "app.intelligence.policies",
    "app.intelligence.interpretation",
    "app.intelligence.signal_registry",
    "app.signals",
]

print("\n🔍 IMPORT CHECK\n")

for m in modules:
    try:
        importlib.import_module(m)
        print(f"✅ {m}")
    except Exception as e:
        print(f"❌ {m} -> {e}")

print("\nDONE\n")