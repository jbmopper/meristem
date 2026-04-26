package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/storage"
)

// seedNamespace is the fixed UUID under which every v1-substrate work item
// derives its id from its slugified title (uuid.NewSHA1 in the v5 sense).
// Two consequences:
//
//   - Reruns of `meristem seed v1` produce the same subject_id per item, so
//     the events writer's deterministic id collapses replays into one row.
//   - Renaming a substrate item changes its id. Treat the title as load-
//     bearing identity; if you want to rephrase, do so through the running
//     system, not by editing this list.
//
// This UUID was generated once and pinned. Do not regenerate it.
var seedNamespace = uuid.MustParse("4d6f3a3a-1ce8-5f6b-8b01-7a64c1e0a3a2")

// v1SubstrateItem is one work_item to seed. Title is the durable identity
// (it feeds the slug that derives subject_id); Body is the human-readable
// description copied from docs/spec.md "v1 Substrate".
type v1SubstrateItem struct {
	Title string
	Body  string
}

// v1SubstrateItems mirrors the bullet list under "v1 Substrate" in
// docs/spec.md. Adding to this list is a deliberate change; once seeded,
// the running system is the authority on what's open and what's done.
//
// Order is preserved across reruns because subject_ids are derived from
// titles, not from list position. Reordering does not move work_items.
var v1SubstrateItems = []v1SubstrateItem{
	{
		Title: "Go repo, Docker-based local dev, Postgres migrations",
		Body:  "Substrate item from docs/spec.md §v1 Substrate: Go repo, Docker-based local dev, Postgres migrations.",
	},
	{
		Title: "Token model: root, scoped client tokens, separation of duties, panic-revoke",
		Body:  "Substrate item from docs/spec.md §v1 Substrate: Token model: root token, scoped client tokens, separation of duties, panic-revoke.",
	},
	{
		Title: "All security primitives from docs/spec.md §Security Primitives",
		Body:  "Substrate item from docs/spec.md §v1 Substrate: All security primitives listed in §Security Primitives — TLS 1.3, HSTS, disk encryption, KMS-wrapped per-connection credentials, loopback Postgres, webhook verification, append-only events with attribution, per-token rate limits, per-connector concurrency caps and circuit breaker, hard-delete /v1/messages/{id}.",
	},
	{
		Title: "work_item, message, artifact, approval, event, token tables and projections",
		Body:  "Substrate item from docs/spec.md §v1 Substrate: work_item, message, artifact, approval, event, token tables and projections.",
	},
	{
		Title: "Append-only events with full attribution",
		Body:  "Substrate item from docs/spec.md §v1 Substrate: Append-only events with full attribution.",
	},
	{
		Title: "Idempotency at every layer per the Idempotency section",
		Body:  "Substrate item from docs/spec.md §v1 Substrate: Idempotency at every layer per the §Idempotency section — HTTP, inbox, jobs, connectors, events, projections, approvals.",
	},
	{
		Title: "POST /v1/inbox/messages accepting multi-modal parts; GET /v1/feed",
		Body:  "Substrate item from docs/spec.md §v1 Substrate: POST /v1/inbox/messages accepting multi-modal parts (text, image, audio, binary); GET /v1/feed.",
	},
	{
		Title: "Worker with job_queue and SELECT … FOR UPDATE SKIP LOCKED",
		Body:  "Substrate item from docs/spec.md §v1 Substrate: Worker with job_queue and SELECT … FOR UPDATE SKIP LOCKED. Long-lived process polling Postgres; no Redis, no NATS.",
	},
	{
		Title: "Convergence loop that drives every work item to a terminal state",
		Body:  "Substrate item from docs/spec.md §v1 Substrate: Convergence loop that drives every work item to a terminal state without owner babysitting. Closes the bounded-patience principle deferred from v0.",
	},
	{
		Title: "Generic HTTP connector with read/write declaration and approval gate",
		Body:  "Substrate item from docs/spec.md §v1 Substrate: Generic HTTP connector with read/write declaration, approval gate on writes, retries, and dead-lettering.",
	},
	{
		Title: "Webhook verification",
		Body:  "Substrate item from docs/spec.md §v1 Substrate: Webhook verification per connection (hmac-sha256, github, stripe, etc.). Unverified inbound rejected and recorded as security events.",
	},
	{
		Title: "Approvals with expiry, re-prompt cadence, and convergence semantics",
		Body:  "Substrate item from docs/spec.md §v1 Substrate: Approvals with expiry, re-prompt cadence, and the convergence semantics in §Approvals and Convergence (default deny, separation of duties, expiry → blocked → escalate to failed).",
	},
	{
		Title: "APNs or Web Push for approval requests, with email/SMS fallback",
		Body:  "Substrate item from docs/spec.md §v1 Substrate: APNs (or Web Push) for approval requests, with email/SMS fallback for push failures.",
	},
	{
		Title: "Minimal web UI: feed, work-item detail, approve/deny, dead-letter view",
		Body:  "Substrate item from docs/spec.md §v1 Substrate: Minimal web UI: feed, work-item detail, approve/deny, dead-letter view.",
	},
	{
		Title: "iPhone Shortcut posting to /v1/inbox/messages",
		Body:  "Substrate item from docs/spec.md §v1 Substrate: iPhone Shortcut posting to /v1/inbox/messages with bearer + Idempotency-Key, surfacing the returned work_item_id.",
	},
	{
		Title: "Full-featured MCP server with parity to REST, including write paths and approval requests",
		Body:  "Substrate item from docs/spec.md §v1 Substrate: Full-featured MCP server with parity to REST, including write paths and approval requests. Extends the v0 read+lifecycle MCP surface.",
	},
	{
		Title: "Nightly Postgres dumps to object storage; documented and rehearsed restore",
		Body:  "Substrate item from docs/spec.md §v1 Substrate: Nightly Postgres dumps to object storage; documented and rehearsed restore. The owner can rebuild the entire system from a backup and a root token.",
	},
	{
		Title: "Single-VM deploy with TLS, disk encryption, and object overflow",
		Body:  "Substrate item from docs/spec.md §v1 Substrate: Single-VM deploy with TLS, disk encryption, and object overflow.",
	},
}

// runSeed dispatches to the per-target seeders. v0 ships exactly one
// target (`v1`); the dispatcher is wired this way so future targets land
// next to it without reshaping main.
func runSeed(ctx context.Context, logger *slog.Logger, args []string) error {
	if len(args) == 0 {
		seedUsage(os.Stderr)
		return fmt.Errorf("seed: missing target")
	}
	switch args[0] {
	case "v1":
		return runSeedV1(ctx, logger, args[1:])
	default:
		seedUsage(os.Stderr)
		return fmt.Errorf("seed: unknown target %q", args[0])
	}
}

// runSeedV1 idempotently appends one work_item.created event per item in
// v1SubstrateItems. Determinism flows from:
//
//  1. seedNamespace + slug(title) → subject_id (uuid.NewSHA1 / v5).
//  2. The events writer hashes (subject_kind, subject_id, kind, payload)
//     into the row id, so reruns with an identical (title, body) collapse
//     to the same row via ON CONFLICT DO NOTHING.
//
// Editing a body without editing the title creates a new event row (the
// payload changed), which would un-collapse the seed. The test in
// seed_test.go pins this; treat the body strings as load-bearing.
func runSeedV1(ctx context.Context, logger *slog.Logger, args []string) error {
	fs := flag.NewFlagSet("seed v1", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dryRun := fs.Bool("dry-run", false, "print the work items that would be seeded without writing")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if *dryRun {
		for _, item := range v1SubstrateItems {
			id := seedSubjectID(item.Title)
			fmt.Fprintf(os.Stdout, "%s\t%s\n", id, item.Title)
		}
		return nil
	}

	if _, _, err := validateStartupSafety(logger); err != nil {
		return err
	}

	cfg, err := storage.LoadConfigFromEnv()
	if err != nil {
		return err
	}
	pool, err := storage.Open(ctx, cfg)
	if err != nil {
		return err
	}
	defer pool.Close()

	writer := app.NewEventWriter()
	authService := auth.NewService(pool, writer)

	systemTok, err := resolveSystemToken(ctx, authService)
	if err != nil {
		return err
	}

	created, replayed, err := seedV1Items(ctx, pool, writer, systemTok)
	if err != nil {
		return err
	}
	logger.Info("seeded v1 substrate",
		slog.Int("created", created),
		slog.Int("replayed", replayed),
		slog.Int("total", len(v1SubstrateItems)),
	)
	fmt.Fprintf(os.Stdout, "seed v1: created=%d replayed=%d total=%d\n", created, replayed, len(v1SubstrateItems))
	return nil
}

type tokenAuthenticator interface {
	Authenticate(context.Context, string) (domain.Token, error)
}

// resolveSystemToken loads the bearer in MERISTEM_TOKEN and refuses to
// proceed unless it is a dedicated, non-root system token. docs/v0.md is
// explicit: "The seed command uses a dedicated `system` token, not root."
func resolveSystemToken(ctx context.Context, service tokenAuthenticator) (domain.Token, error) {
	secret := os.Getenv("MERISTEM_TOKEN")
	if secret == "" {
		return domain.Token{}, fmt.Errorf("seed v1: MERISTEM_TOKEN with a system-source bearer is required (mint one with `meristem tokens create --source system --name seed`)")
	}
	tok, err := service.Authenticate(ctx, secret)
	if err != nil {
		return domain.Token{}, err
	}
	if tok.Source != domain.SourceSystem {
		return domain.Token{}, fmt.Errorf("seed v1: MERISTEM_TOKEN must be source=system, got %q (root is deliberately not accepted)", tok.Source)
	}
	if tok.IsRoot {
		return domain.Token{}, fmt.Errorf("seed v1: MERISTEM_TOKEN must be a dedicated system token, not root")
	}
	return tok, nil
}

// seedV1Items is the pool-driven core of `meristem seed v1`. Split out from
// runSeedV1 so an integration test can drive it against a real pool
// without re-parsing flags or reading env. Returns (created, replayed).
//
// Each item lands in its own transaction. Per-item atomicity is enough:
// every item is independently idempotent, so a partial run followed by a
// rerun converges to the same end state (the failed items retry, the
// already-applied items collapse via the deterministic id).
func seedV1Items(ctx context.Context, pool *pgxpool.Pool, writer *events.Writer, actor domain.Token) (created, replayed int, err error) {
	for _, item := range v1SubstrateItems {
		fresh, appendErr := appendSeedItem(ctx, pool, writer, actor, item)
		if appendErr != nil {
			return created, replayed, fmt.Errorf("seed v1 %q: %w", item.Title, appendErr)
		}
		if fresh {
			created++
		} else {
			replayed++
		}
	}
	return created, replayed, nil
}

func appendSeedItem(ctx context.Context, pool *pgxpool.Pool, writer *events.Writer, actor domain.Token, item v1SubstrateItem) (bool, error) {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	subjectID := seedSubjectID(item.Title)
	_, fresh, err := writer.Append(ctx, tx, events.Spec{
		SubjectKind:  domain.SubjectWorkItem,
		SubjectID:    subjectID,
		Kind:         domain.EventWorkItemCreated,
		Source:       domain.SourceSystem,
		ActorTokenID: &actor.ID,
		Payload: map[string]any{
			"title": item.Title,
			"body":  item.Body,
			"state": domain.WorkItemCaptured,
		},
	})
	if err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return fresh, nil
}

// seedSubjectID returns the deterministic subject_id for a v1 substrate
// item. It is a v5 UUID under seedNamespace, hashing the slug of title.
// Slugs (not raw titles) are the input so trivial whitespace/punctuation
// edits do not silently fork the identity.
func seedSubjectID(title string) uuid.UUID {
	return uuid.NewSHA1(seedNamespace, []byte(slugify(title)))
}

// slugify is a small, deterministic title-to-slug function. Lowercase,
// keep [a-z0-9], replace runs of anything else with a single hyphen, trim
// leading/trailing hyphens. It is intentionally not RFC-anything: the
// only contract is "stable across runs of this binary", which a unit
// test pins.
func slugify(s string) string {
	s = strings.ToLower(s)
	var b strings.Builder
	b.Grow(len(s))
	prevHyphen := true // suppress leading hyphens
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
			prevHyphen = false
		default:
			if !prevHyphen {
				b.WriteByte('-')
				prevHyphen = true
			}
		}
	}
	out := b.String()
	return strings.TrimRight(out, "-")
}

// seedItemsFingerprint returns a stable hash over the in-binary item list
// (title + body). The unit test compares this against a pinned constant
// so an accidental edit to v1SubstrateItems is loud, not silent.
func seedItemsFingerprint() string {
	h := sha256.New()
	for _, item := range v1SubstrateItems {
		_ = json.NewEncoder(h).Encode([2]string{item.Title, item.Body})
	}
	sum := h.Sum(nil)
	return fmt.Sprintf("%x", sum)
}

func seedUsage(w io.Writer) {
	fmt.Fprint(w, `usage:
  MERISTEM_TOKEN=mrs_<system> meristem seed v1 [--dry-run]

Seeds the v1 substrate backlog from docs/spec.md into the running system as
work_items, attributed to the supplied system-source token. Reruns are
no-ops; the same (title, body) pair always produces the same row.
`)
}
