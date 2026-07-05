# Owner quickstart (Codex draft)

Command spine from built checkout to live operation; agents use worktrees.

```bash
export MERISTEM_DATABASE_URL='postgres://meristem:meristem@localhost:5432/meristem?sslmode=disable'
export API='http://127.0.0.1:8080'
export BIN="${BIN:-go run ./cmd/meristem}"
```

## 1. Bootstrap and worker worktrees
```bash
scripts/bootstrap.sh
scripts/prepare-agent-worktree.sh --target codex
scripts/prepare-agent-worktree.sh --target claude-code-gui
MERISTEM_DATABASE_URL="$MERISTEM_DATABASE_URL" \
  scripts/provision-assistant-access.sh --targets codex,claude-code-gui --print-remote
# -> .meristem/*.token plus secret-free wrappers under .meristem/generated/
```

## 2. Start or verify the API
```bash
"$BIN" safety check
"$BIN" migrate
MERISTEM_HTTP_ADDR='127.0.0.1:8080' "$BIN" api
# in another shell:
curl -fsS "$API/readyz"
# -> {"database":"ok","policy_profile":"steady","safety":"ok",...}
```

## 3. Mint the operator token
```bash
MERISTEM_TOKEN="$(tr -d '\n' < .meristem/root.token)" \
  "$BIN" tokens create --name owner-operator --source human \
  --scopes policy_profile.switch,registry.write,work_items.read,work_items.write,feed.read,feed.read_assigned
# -> id=... source=human secret=mrs_...
```

Put the printed `secret=` value in `.meristem/operator.token` with mode 0600.
Root remains for mint, revoke, and panic-revoke only.

## 4. Switch to bring-up
```bash
curl -fsS -X POST "$API/v1/policy-profile" \
  -H "Authorization: Bearer $(tr -d '\n' < .meristem/operator.token)" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: owner-profile-bring-up-001' \
  --data '{"profile":"bring-up"}'
# -> {"profile":"bring-up","fingerprint":"...","switched":true}
curl -fsS "$API/readyz"
# -> ... "policy_profile":"bring-up" ...
```

## 5. Run the first worker tick
```bash
MERISTEM_TOKEN="$(tr -d '\n' < .meristem/seed.token)" "$BIN" worker --once
# -> worker --once: scanned=N emitted=M already_recorded=K ... dispatch_requested=D ...
```

Use a separate source=`system` worker token later for split audit identity.

## 6. Read readiness and feeds
```bash
curl -fsS "$API/v1/backlog/readiness?limit=200" \
  -H "Authorization: Bearer $(tr -d '\n' < .meristem/operator.token)"
curl -fsS "$API/v1/feed?projection=owner-attention&limit=20" \
  -H "Authorization: Bearer $(tr -d '\n' < .meristem/operator.token)"
curl -fsS "$API/v1/feed?projection=dispatch&wait=30s&limit=50" \
  -H "Authorization: Bearer $(tr -d '\n' < .meristem/operator.token)"
# -> watcher responses include next_cursor; reuse it only with the same projection
MERISTEM_TOKEN="$(tr -d '\n' < .meristem/operator.token)" "$BIN" feed --limit 20
MERISTEM_TOKEN="$(tr -d '\n' < .meristem/operator.token)" "$BIN" feed --watch
```

## 7. Export corpus and replay-check projections
```bash
"$BIN" export > .meristem/corpus.jsonl
# share corpus.jsonl, not raw dumps
"$BIN" rebuild --verbose
# -> rebuild complete with 0 mismatched projection tables
scripts/snapshot-db.sh create
scripts/snapshot-db.sh list .meristem/backups/meristem-YYYYMMDDTHHMMSSZ.dump
```

Archive replay is an operator lane: restore private dumps into scratch
Postgres, point `MERISTEM_DATABASE_URL` there, then run `"$BIN" rebuild
--verbose` and `"$BIN" export`.

## 8. Fast-forward `footgun`
```bash
"$BIN" git fetch origin
"$BIN" git checkout footgun
"$BIN" git merge --ff-only v1
"$BIN" git push origin footgun
"$BIN" git checkout v1
# HTTPS trouble: "$BIN" git push git@github.com:jbmopper/meristem.git footgun
```

## 9. Operate
Watch `owner-attention` for escalations and review gates. R5 is landed:
worker-proposed cultivars use `POST /v1/work-items/{id}/cultivar-activations`
or MCP `registry.activate_cultivar`; activation requires same-tree authority
plus human review and denies rootstock self-modification.

```bash
curl -fsS -X POST "$API/v1/tokens/revoke-all" \
  -H "Authorization: Bearer $(tr -d '\n' < .meristem/root.token)" \
  -H "Idempotency-Key: panic-revoke-$(date +%s)"
curl -fsS -X POST "$API/v1/policy-profile" \
  -H "Authorization: Bearer $(tr -d '\n' < .meristem/operator.token)" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: owner-profile-steady-001' \
  --data '{"profile":"steady"}'
```
