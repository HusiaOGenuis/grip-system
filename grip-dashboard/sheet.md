# ✅ PART 2 — One‑page cheat sheet (printable, glanceable)

This is the **anti‑confusion page** you keep open while working.

---

## 🧭 GRIP — One‑Page Cheat Sheet

### System Flow (never changes)

### Where do things live?

| Thing | Where |
|-----			|------|
| Decision logic | `grip/engine/*` |
| API enforcement | `grip/main.py` |
| System truth | `README.md` |
| Procedures | `RUNBOOK.md` |
| Auth | Supabase Auth |
| Secrets (Edge) | Supabase secrets |
| Secrets (GRIP) | `.env` / Render env |
| Dashboard calls | `/api/*` only |

---

### What never happens

- Browser → GRIP ❌  
- Browser holds API keys ❌  
- GRIP knows about Supabase ❌  

---

### How to test quickly

| Test | File |
|----|-----|
| Local GRIP | RUNBOOK → “Local GRIP” |
| Edge → GRIP | RUNBOOK → “End‑to‑End” |
| Architecture | README |

---

### If something feels confusing

Ask:
1. Is this **what the system is**? → README  
2. Is this **what I do**? → RUNBOOK  
3. Is it neither? → Don’t write it yet

---

# ✅ PART 3 — Your specific question answered clearly

> **“Should these be included and commented out?”**

❌ **No. Never.**

- Do **not** comment out commands
- Do **not** prefix with `#`
- Do **not** keep “reference commands” disabled

✅ If a command is runnable → fenced code block in RUNBOOK  
✅ If it’s conceptual → prose/diagram in README  
✅ If it’s neither → don’t include it

That rule alone prevents 90% of documentation entropy.

---

## Final reassurance

You are not struggling with complexity —  
you are **successfully crossing the boundary where structure replaces memory**.

At this point, your system is:
- architecturally sound
- operationally documented
- cognitively manageable

When you’re ready, the next steps are purely mechanical:
- ✅ Dashboard → Edge wiring
- ✅ DNS binding
- ✅ Final security lock

Just tell me which one you want to do next.