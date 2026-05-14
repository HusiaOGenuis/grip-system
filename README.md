# GRIP — System Overview (Canonical)

## Purpose
## Purpose

GRIP is a **trust and readiness layer for structured data used by AI and agentic systems**.

Modern AI systems increasingly act on structured data — making decisions, triggering actions, and coordinating workflows. GRIP exists to ensure that this data is **reliable, explainable, and governable before it is consumed by AI**.

GRIP provides infrastructure‑level capabilities to:
- assess data confidence and completeness
- detect anomalies and integrity risks
- enforce explicit policy constraints
- produce auditable, traceable outcomes
- gate downstream automation based on trust thresholds

GRIP does not make business decisions on behalf of users or models.
Instead, it establishes whether structured data is **fit for automated or agentic use**, and under what conditions.

This makes GRIP an enabling layer for:
- AI agents
- automated decision pipelines
- regulated data workflows
- safety‑critical or compliance‑sensitive systems
## System Map (Authoritative)

This is the **only valid request flow**:

Browser / Dashboard  
→ Supabase Edge Function  
→ GRIP API (FastAPI on Render)

### Hard rules
- Browsers **never** call GRIP directly
- Browsers **never** hold API keys
- GRIP **only** trusts requests from the Edge Function
- The Edge Function is the **security choke‑point**

If a request violates this flow, it is invalid by design.

---

## Component Responsibilities

### GRIP API (FastAPI)
- Hosts all decision logic
- Enforces `X‑API‑Key`
- Stateless
- No knowledge of Supabase, dashboards, or browsers

Location:
- `grip/main.py`
- `grip/engine/*`

---

### Supabase Edge Function
- Publicly reachable API surface
- Requires Supabase Auth JWT
- Injects `X‑API‑Key` into requests to GRIP
- Isolates all secrets from browsers

Location:
- `grip-dashboard/supabase/functions/api/index.ts`

---

### Dashboard / Browser
- User interaction only
- Uses Supabase Auth
- Calls `/api/*` endpoints
- Holds **no secrets**

Location:
- Static frontend (outside GRIP core)

---

## Secrets & Trust Boundaries

### GRIP_API_KEY
- Used by GRIP to authenticate callers
- Stored in:
  - `.env` (local development)
  - Render environment variables (production)
  - Supabase Edge secrets
- Never exposed to browsers

---

### Supabase Service Role Key
- Supabase‑internal capability
- May be used by Edge Functions only
- Must never exist in:
  - GRIP `.env`
  - dashboard code
  - browser storage

---

## What This Document Is NOT

- Not a runbook
- Not a deployment guide
- Not a troubleshooting reference
- Not a list of commands

---

## Canonical Status

This document is **canonical**.
If behavior contradicts this document, the behavior is wrong.

Changes to this document imply **architectural changes** and must be deliberate.
## Conceptual Interaction: GRIP and AI Agents

An AI agent does not consume raw structured data directly.

Instead, the agent queries GRIP to determine whether the data is safe to act upon.

Example flow:

1. An AI agent requests to act on a structured dataset (e.g. customer record, transaction batch).
2. The agent submits the data context to GRIP.
3. GRIP evaluates:
   - data completeness
   - anomaly signals
   - confidence thresholds
   - applicable policy constraints
4. GRIP returns a readiness assessment, including:
   - trust score
   - permitted automation scope
   - escalation requirements (if any)
5. The agent proceeds only within the bounds declared by GRIP.

In this model, GRIP functions as a **trust gate** between data and autonomous action.
