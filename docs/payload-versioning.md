# Payload schema versioning

Status: convention, adopted 2026-07-06 (work item `54e6d5d4`), decided while
the log is ~2k events so projectors never need archaeology. Owner human-ack
gates adoption by the first projector.

## The problem

Projectors must fold every historical payload shape forever. Without a
versioning convention, evolving a payload's shape forces projectors to grow
undocumented legacy-parsing branches, and rebuild safety decays silently
with each schema change.

## The convention

1. **Identity-bearing payloads MAY carry `"payload_version": N`** (integer,
   ≥ 2). Absence means version 1 — every event written before this
   convention is retroactively version 1 by definition, so nothing in the
   existing log changes meaning.
2. **`payload_version` participates in the deterministic event id** like
   any other payload field. A version bump on otherwise-identical content
   is a new identity — correct, since consumers interpret it differently.
3. **Writers bump the version only on breaking shape changes** (field
   renamed, type changed, meaning changed). Additive optional fields do not
   bump: projectors must already tolerate unknown fields (Postgres JSONB
   round-trips them) and absent optionals.
4. **Projectors dispatch on version explicitly.** The sanctioned pattern is
   a top-level switch per kind:

   ```go
   switch payloadVersion(event.Payload) { // absent => 1
   case 1:
       return applyV1(ctx, tx, event)
   case 2:
       return applyV2(ctx, tx, event)
   default:
       return fmt.Errorf("%s: unknown payload_version %d", event.Kind, v)
   }
   ```

   Unknown versions **fail the transaction** (fail closed): an old binary
   must not misfold an event written by a newer one. This is the projector
   mirror of the expand-safe migration rule.
5. **Version dispatch is forever.** `applyV1` is never deleted while any
   v1 event exists in any log the binary might fold — which, for an
   append-only system, means never. This is the honest cost of replay, paid
   in one well-shaped function per version instead of interleaved
   conditionals.

## Deferred, considered

- **Snapshot/checkpoint folding** (fold to seq N, persist snapshot, replay
  the tail): the escape hatch if rebuild duration ever matters. Not needed
  at current scale; the convention above keeps it possible.
- **Upcasting at read time** (rewriting old payloads to the newest shape in
  memory before one shared apply): rejected for now — it hides which shape
  the log actually holds, and the export corpus should show real bytes.

## Enforcement

- AGENTS.md → Techniques carries the one-bullet distillation; the dogma
  conformance map links it to this document and to the first
  version-dispatching projector test when one exists.
- Until a second version of any payload ships, this convention is
  documentation only — zero runtime cost.
