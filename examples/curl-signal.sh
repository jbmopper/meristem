#!/usr/bin/env bash
# Worked example: post a legacy.work_spec.v1 signal.
#
# Treat this as the canonical "did the deploy work?" smoke test that any
# integrator can run without writing code. It demonstrates:
#
#   - Bearer auth (Authorization header).
#   - Idempotency-Key header (required on every POST per docs/v0.md).
#   - The legacy.work_spec.v1 schema (the contract jay, ns_obv, and
#     clinical-demo all target).
#   - The dedupe_key field, which collapses repeated signals about the
#     same logical work into one work_item.
#
# Usage:
#   MERISTEM_TOKEN=wln_... examples/curl-signal.sh
#   MERISTEM_TOKEN=wln_... MERISTEM_URL=http://localhost:8080 examples/curl-signal.sh
#
# Re-running the script with the same Idempotency-Key returns the
# original 201 response with header `Idempotency-Replayed: true`.
# Re-running with the same dedupe_key but a fresh Idempotency-Key
# creates a new signal row that links to the same work_item (semantic
# dedupe).

set -euo pipefail

MERISTEM_URL="${MERISTEM_URL:-http://localhost:8080}"
MERISTEM_TOKEN="${MERISTEM_TOKEN:-}"
DEDUPE_KEY="${MERISTEM_DEDUPE_KEY:-example:repair:retry-budget-001}"
IDEMPOTENCY_KEY="${MERISTEM_IDEMPOTENCY_KEY:-$(uuidgen 2>/dev/null || python3 -c 'import uuid; print(uuid.uuid4())')}"

if [[ -z "$MERISTEM_TOKEN" ]]; then
    cat >&2 <<EOF
MERISTEM_TOKEN is required.

Mint one against your local stack:
  MERISTEM_TOKEN=\$(cat .meristem/root.token) \\
    go run ./cmd/meristem tokens create --name example --source agent

Then re-run:
  MERISTEM_TOKEN=wln_... \$0
EOF
    exit 2
fi

read -r -d '' BODY <<'JSON' || true
{
  "kind": "repairable_failure",
  "dedupe_key": "__DEDUPE__",
  "source": {
    "kind": "system_event",
    "identifier": "example:smoke-test:001"
  },
  "work_spec": {
    "schema_version": "legacy.work_spec.v1",
    "kind": "repair",
    "dedupe_key": "__DEDUPE__",
    "title": "Honor worker retry budget",
    "priority": "P2",
    "objective": "The worker stops retrying past the configured budget.",
    "details": "Smoke-test signal posted by examples/curl-signal.sh. Replace with a real diagnosis.",
    "acceptance_criteria": [
      "the budget is honored under sustained failure",
      "no event loop is observed in the logs"
    ],
    "labels": ["example", "smoke-test"]
  }
}
JSON

BODY="${BODY//__DEDUPE__/$DEDUPE_KEY}"

echo "POST $MERISTEM_URL/v1/signals" >&2
echo "  Idempotency-Key: $IDEMPOTENCY_KEY" >&2
echo "  dedupe_key:      $DEDUPE_KEY" >&2
echo >&2

# -i prints headers so the operator can see Idempotency-Replayed and
# the response status. The body is JSON; pipe through `jq` if you have
# it for prettier output.
curl -sS -i -X POST "$MERISTEM_URL/v1/signals" \
    -H "Authorization: Bearer $MERISTEM_TOKEN" \
    -H "Idempotency-Key: $IDEMPOTENCY_KEY" \
    -H "Content-Type: application/json" \
    --data "$BODY"
echo
