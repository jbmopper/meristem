# Listener-Based Capability Routing

> Status: owner-directed architecture record. The normative invariants are in
> [`spec.md`](spec.md), especially **Capability Routing and External Agent
> Execution**. Live work items and events remain the execution backlog.

## Purpose

Meristem should keep work moving without requiring the owner to bring model
sessions up, relay handoffs, or manually restart a review loop. The missing
capability is not reviewer assignment by itself. It is robust activation and
continuation across independently operated model applications.

The normal topology is a small set of stable, complementary listeners. Each
listener is an attributed Meristem client associated with an application or
provider session. When addressed work arrives, the listener claims the work
and decides how to execute it efficiently.

## Terms

- **Principal**: the stable authenticated client or listener. It answers who
  acted and through which application.
- **Capability demand**: bounded work that needs an eligible actor, such as
  implementing a change or independently reviewing one exact commit.
- **Assignment**: the temporary relationship between a principal and a work
  item, protected by a finite lease.
- **Role**: ad-hoc behavior for the assignment, such as writer, reviewer, or
  critic. It is data, not a permanent identity or schema enum.
- **Lens**: the filtered context and feed view appropriate to the assignment.
- **Execution adapter**: how the listener performs the work: inline, local
  subagent/subtask, direct model API, connector, or fresh process.
- **Activation**: waking or invoking an eligible actor. Activation does not
  imply that a new operating-system process was created.

## Routing Loop

```text
desired capability on a work item or artifact
  -> deterministic dispatch and addressed delivery
  -> eligible listener wakes and atomically claims a finite lease
  -> listener selects a context-efficient execution adapter
  -> attributed result, failure, or handoff is appended
  -> deterministic reducer accepts, retries, reassigns, or escalates
```

The event log and assignment projection are truth. Push, SSE, a bridge, or an
app notification only wakes the listener. Cursor resume, deduplication, claim
generation, and redelivery must make restart indistinguishable from ordinary
continuation.

## Complementary Review

Review is an instance of capability routing:

- The demand names one exact artifact or commit.
- The implementation author is ineligible.
- Policy may require a different principal or model provider to diversify
  failure modes.
- An existing complementary listener is preferred.
- The listener may review directly or spawn local subagents/subtasks.
- A direct API call or fresh process is a fallback when no eligible app
  listener can serve the assignment within its patience budget.
- Only an attributed, generation-fenced verdict for the exact artifact counts
  as a convergence signal.

This makes the useful invariant "independent complementary review happened,"
not "Meristem launched a reviewer process."

## Identity and Token Exchange

Stable identity and temporary role authority are different concerns.

The stable listener credential identifies the application/session principal.
Assignment, role, lens, resource, and duration are temporary. Token exchange
is the intended mechanism for deriving a short-lived assignment credential
when the stable credential should not carry the necessary authority directly.

An exchanged credential must:

- be no broader than its source authority;
- be bound to the target audience/resource and assignment tree or artifact;
- expire no later than the assignment lease;
- preserve an explicit delegation/actor chain for attribution;
- omit unrelated authority held by either the source or target listener;
- support ad-hoc role changes without minting a new permanent agent identity.

The precise claim vocabulary remains a separate auth design decision. MCP
`clientInfo` and other client metadata are observational and never confer
authority.

## Execution Adapters

The listener, not the deterministic core, selects among allowed adapters:

1. **Inline** — use the current context when the work is small.
2. **In-app subagent or subtask** — fan out while keeping the listener as the
   accountable principal unless the child receives a narrowed Meristem
   credential.
3. **Direct model API** — useful when programmatic invocation, reproducibility,
   or unattended fallback matters more than app-priced usage.
4. **Fresh process** — an optional vendor adapter requiring lifecycle,
   worktree, credential, budget, and cleanup controls.

Core records the demand, lease, evidence, and outcome. It must not hard-code a
particular editor, application, provider, or process supervisor.

## Robustness Contract

A routed workflow is not complete merely because an event was addressed.
Correctness requires:

- filter-bound durable cursor resume;
- one logical wake despite redelivery;
- atomic claim and visible contention;
- a completion, failure, or yield receipt;
- exact-artifact and assignment-generation fencing where relevant;
- lease expiry and bounded patience;
- reassignment or an allowed fallback adapter;
- hand-to-human only after automated recovery is exhausted.

The first release smoke should exercise a complete loop: one provider
implements, a complementary provider listener wakes and claims the exact
review, the reviewer records a verdict, and the implementer is woken for
revision or terminal convergence. Restart or failed-wake recovery must be part
of the proof. The owner should not relay any handoff.

## Relationship to Reviewer Spawn

The existing `Reviewer Spawn` branch is research, not the default
architecture. Preserve and reassess its reusable invariants:

- exact-artifact and assignment-generation fencing;
- structural self-review exclusion;
- durable capacity/failure outcomes;
- bounded retry and cleanup evidence.

Do not merge its process-pool assumptions, single-use credential lifecycle, or
migrations unchanged. Rebase any reusable pieces onto the listener-first
capability-routing design. Fresh-process spawning remains a fallback adapter.

## Self-Growing Requirement

Repeated owner relays, expired claims, missed wakes, and manual session
bring-up are capability-gap signals. Meristem should be able to turn that
evidence into a proposed work item and architecture correction without the
owner first naming the solution.

The proposal remains probabilistic; adoption remains gated. A deterministic
reducer records which evidence was considered, whether the proposed checks
were satisfied, and when owner authority is required. Self-growing means the
system proposes the missing capability and drives its work graph—not that it
self-approves new authority.

## Live Work Items

The durable backlog for this direction is:

- `40d60482-7d5b-597e-b8f7-178f4d8e65ff` — listener-first capability-routing
  umbrella; human-blocked design gate.
- `37d442e8-4132-54a2-8a40-9e59d12cd357` — assignment-bound token exchange
  for ad-hoc roles.
- `d46c097a-b9b4-5432-b7b5-b287a0d9223f` — unattended
  complementary-provider review through existing listeners.
- `7cd1ecce-92ce-5fd2-8287-b9bac773452e` — disposition the existing Reviewer
  Spawn research into reusable execution-adapter invariants.
- `7a222b6c-0609-5f60-9317-a84fbe7cf3be` — self-growing proposal generation
  from repeated coordination friction.

Historical anchors remain visible rather than being rewritten:

- `ee916614-4a39-5876-a983-d97c2cb2804f` — process-centric Reviewer Spawn
  proposal and unmerged research branch.
- `70fec8e7-0996-5e9d-bf31-491d95096f6a` — bidirectional subagent-session
  design.
- `c7160148-22ca-5d2e-aa4b-3378f84bbf0d` — subagent session through existing
  MCP primitives.
- `53af47e8-1bb6-51a3-aefa-f8bbdf10cf0c` — self-building gate for bring-up.
