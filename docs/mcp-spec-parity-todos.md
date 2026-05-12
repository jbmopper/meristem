# MCP ↔ spec parity — open items

Tracking gaps between `docs/spec.md`, `docs/v0.md`, and the implementation (`internal/mcp`, `internal/api`, `internal/access`). Created as a single backlog artifact; check items off when addressed.

## Documentation fixes

- [ ] **`docs/v0.md` sequencing typo** — Step 8 says “eight tools” but the MCP surface lists **eleven** tools. Update the sequencing line to match the MCP Tool Surface section.

- [ ] **`docs/spec.md` v1 MCP checklist wording** — The line “MCP server with REST parity ✅ for read/triage paths; write paths with approval requests open” conflates **substrate work_item writes** (implemented over MCP) with **approval-gated connector writes** (not shipped). Tighten wording so v1 status reflects: plain MCP writes vs connector/approval writes.

## Spec vs implementation

- [ ] **REST↔MCP surface parity (explicit)** — `docs/spec.md` states that every REST operation has an MCP tool. Today **no MCP tools** exist for `POST /v1/signals` and **`GET /v1/feed/stream`** (SSE); parity is intentionally partial. Either document the exception list or add tools/transports where appropriate.

- [ ] **Idempotency on MCP mutations** — HTTP POSTs use `Idempotency-Key` middleware; MCP tool calls do not. If “idempotency at every layer” applies to MCP, specify mechanism (e.g. optional key in tool args + shared store) and implement; otherwise record an explicit spec carve-out for MCP.

- [ ] **Response shape parity** — Align minor deltas where desired (e.g. `POST /v1/inbox/messages` returns `captured_at`; `inbox.capture` omits it unless callers rely on it).

## v1 / future (when substrate ships)

- [ ] **v1 acceptance MCP capabilities** — Spec acceptance mentions MCP flows for **artifacts**, **approval requests**, and **observing approval decisions**. Track implementation once approvals and artifacts exist; until then keep acceptance criteria scoped or marked contingent.

---

_Reconcile this file with live `work_item`s in meristem when the inbox loop is the canonical backlog; until then this doc is the durable checklist._
