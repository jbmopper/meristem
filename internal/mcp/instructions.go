package mcp

// serverInstructions is returned as the top-level `instructions` string in the
// MCP initialize result. Compliant clients (Claude Code, Cursor, etc.) inject
// it into the connecting agent's system prompt, so it is the one chance to
// onboard an agent to meristem's domain model before it starts calling tools.
//
// Keep it tight and transport-agnostic: the same text ships over stdio and the
// provider-facing HTTP /mcp gateway, whose profiles expose only a subset of the
// tools below. Phrase tool references as "if available" rather than promising a
// surface a restricted credential may lack. This constant is intentionally the
// only onboarding channel; do not edit tools.go to duplicate it.
const serverInstructions = `meristem is an event-backed coordination and work-tracking plane for humans and agents. This MCP connection is the live source of truth for state, progress, and handoff — not chat history. Read current state from meristem before acting, and record progress back into it.

Tool availability varies by credential and profile: some connections expose only reads or a narrow tracker-mutation subset. If a tool named below is absent, it is out of scope for your credential, not missing. Tool names may be dot- or underscore-spelled (work_items.get or work_items_get); use whichever this client advertises — they route to the same operations.

Core vocabulary:
- Work item lifecycle states: captured, triaged, planned, awaiting_approval, running, blocked, done, failed, canceled. Transitions are server-validated; an illegal move is rejected. An item cannot advance toward execution or success until it declares suggested_convergence_checks.
- human_review_status gates whether an item may be worked: blocked means human input is required before proceeding or claiming convergence; waved_through means normal permission to proceed within scope; approved means explicit human clearance.
- Projections / feeds: feed_read returns feed-visible events; named projections include activity (chronological narrative), owner-attention (what needs a human), and dispatch (worker-facing).
- Approvals: side effects that need sign-off are parked in awaiting_approval via an approval request; a human non-root token decides. Do not auto-approve external effects.
- Registry: a cultivar is a reusable worker profile (tropism + scopes + limits); a tropism is the convergence reducer that decides when an item's checks are satisfied.
- Patience and escalation: non-terminal items carry a bounded patience budget; when it is exhausted the item breaches and escalates (e.g. hand_to_human) rather than stalling silently.

Conventions:
- Every mutation tool requires an idempotency_key. Generate a fresh UUID per logical action; reuse the same key only when retrying the identical call with identical arguments.
- Visibility may be scoped to an assigned work-item tree. Items you cannot see may simply be out of your scope, not absent from the system.

Where to start:
- backlog_readiness for an overview of what is ready, blocked, running, or stale.
- work_items_get / work_items_list to read current state.
- work_items_append_event to record progress notes.
- work_items_transition to move an item between lifecycle states.

Worker etiquette:
- Before beginning work on an assigned item, append a worker.started event, then transition it to running if appropriate.
- Do not claim convergence (done) on an item whose human_review_status is blocked; block and surface the required decision instead.`
