# GRIP — Deployment Specification
## Version v1.0.0
### Label: Decision‑Grade Baseline

---

## 1. Purpose

This document defines the authoritative deployment requirements and
integrity guarantees for the GRIP (Governance, Risk, Integrity Platform)
decision engine.

Version v1.0.0 is designated as the **Decision‑Grade Baseline**.

---

## 2. Deployment Preconditions (MUST HOLD)

### 2.1 Structural Integrity
- Exactly one `/decision` endpoint exists
- Exactly one `/override` endpoint exists
- No duplicate FastAPI route registrations
- No dead or unused engine files
- No overlapping decision logic

### 2.2 Decision Contract
- All decisions are represented by `DecisionResult`
- `trace_id` is mandatory and immutable
- Decisions are never mutated post‑persistence

### 2.3 Confidence & Envelope
- Confidence is computed deterministically
- Envelope is derived exclusively from confidence
- No override bypasses envelope semantics

### 2.4 Override Governance
- Static override policy is enforced
- Confidence‑aware override guard is enforced
- Overrides require valid attestation
- All overrides reference an existing `trace_id`

---

## 3. Traceability Guarantees

- The `trace_id` is the primary key for decisions
- Overrides reference decisions exclusively via `trace_id`
- Effective verdicts are resolved per trace
- All records are append‑only

---

## 4. Golden Decision Reference

A canonical reference decision is maintained at: