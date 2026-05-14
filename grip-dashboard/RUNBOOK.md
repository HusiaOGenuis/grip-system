# GRIP — Operational Runbook

This document contains **procedural steps only**.
System intent and architecture are defined in `grip-system-clean/README.md`.

---

## Local GRIP: Run and Verify

### Purpose
Confirm that the GRIP API:
- starts correctly
- enforces `X-API-Key`
- returns a valid decision

---
### Preconditions

- Commands must be run from the project root:
  `grip-system-clean`
- The `grip/` package must be visible in the current working directory
###REDIRECT
cd C:\Users\iamsu\grip-system-clean\grip-dashboard
cd C:\Users\iamsu\grip-system-clean\grip-dashboard
notepad++ 
### 1. Start GRIP locally

From `grip-system-clean`:

```powershell
uvicorn grip.main:app --reload
``


powershell
conda activate grip
uvicorn grip.main:app --reload

Invoke-RestMethod `
  -Uri "http://127.0.0.1:8000/decision" `
  -Method POST `
  -ContentType "application/json" `
  -Body '{ "score": 82 }'
  
Invoke-RestMethod `
  -Uri "http://127.0.0.1:8000/decision" `
  -Method POST `
  -Headers @{ "X-API-Key" = "GRIP_API_KEY_VALUE" } `
  -ContentType "application/json" `
  -Body '{ "score": 82 }'

Invoke-RestMethod `
  -Uri "https://txlddhgevpssgkqtosnz.supabase.com/functions/v1/api/decision" `
  -Method POST `
  -Headers @{
    "Authorization" = "Bearer SUPABASE_JWT"
  } `
  -ContentType "application/json" `
  -Body '{ "score": 82 }'
