// rebuild folds the entire event log through every registered projector
// into an isolated sandbox schema, then diffs the resulting projection
// rows against the live ones. This is v0 acceptance test #6 made
// runnable: if any projection table can be reproduced from `events`
// alone, the "events is the system" invariant holds for that table; if
// any cannot, either a projector is non-deterministic, an event was
// missed, or someone wrote a non-`events` row outside a projector.
//
// The rebuild never mutates live data. It runs inside one transaction
// that ROLLBACK-s at the end, regardless of outcome. The sandbox schema
// (default `meristem_rebuild`) lives only for the duration of the
// transaction; nothing is left behind.
//
// The fold is deterministic: events are streamed in events.seq order, and
// projectors must be pure with respect to event payload (see
// internal/projections/projections.go). A successful rebuild means
// every projection row in `public.*` has a corresponding event in
// `public.events` that produces it.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/projections"
	"github.com/jbmopper/meristem/internal/storage"
)

// projectionTables is the canonical list of tables derived from `events`.
// Order matters only for stable diff output; the rebuild itself is
// driven by event ordering, not table ordering. Adding a new projection
// table means adding it here AND wiring its projector in
// internal/app/projectors.go; the two lists are siblings, not parent/
// child.
var projectionTables = []string{
	"tokens",
	"work_items",
	"work_item_relations",
	"messages",
	"message_parts",
	"idempotency_keys",
	"signals",
	"deterministic_errors",
	"convergence_verdicts",
	"active_policy_profile",
	"tropisms",
	"cultivars",
	"projections",
	"approvals",
	"http_connector_actions",
	"nodes",
	"registry_snapshot_state",
	"command_queue",
	"crossnode_outcome_observations",
	"crossnode_outcome_cursors",
	"spoke_state",
	"oauth_clients",
	"oauth_authorization_codes",
	"oauth_authorization_requests",
	"oauth_grants",
	"oauth_access_tokens",
	"oauth_refresh_tokens",
}

// rebuildScratchTables are event-caused operational tables that projectors
// may write while replaying, but whose mutable runtime fields are not part of
// the projection honesty diff. job_queue is the current example: enqueue rows
// are caused by dispatch.requested, while lease state is worker coordination.
var rebuildScratchTables = []string{
	"job_queue",
	"outbox_events",
}

func runRebuild(ctx context.Context, logger *slog.Logger, args []string) error {
	fs := flag.NewFlagSet("rebuild", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	schemaName := fs.String("schema", "meristem_rebuild", "sandbox schema name (dropped at end)")
	verbose := fs.Bool("verbose", false, "log per-table row counts and content hashes")
	if err := fs.Parse(args); err != nil {
		rebuildUsage(os.Stderr)
		return err
	}
	// Defensive: schema name is interpolated into DDL because pgx does
	// not parameterize identifiers. Reject anything that is not a plain
	// identifier so a stray flag value can't smuggle SQL.
	if !looksLikeIdentifier(*schemaName) {
		return fmt.Errorf("rebuild: --schema must be a plain identifier, got %q", *schemaName)
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

	registry := app.NewProjectionRegistry()

	report, err := rebuildAndDiff(ctx, pool, registry, *schemaName, logger, *verbose)
	if err != nil {
		return err
	}

	logger.Info("rebuild complete",
		slog.Int("events_replayed", report.eventsReplayed),
		slog.Int("tables_checked", len(projectionTables)),
		slog.Int("tables_mismatched", len(report.mismatches)),
	)

	if len(report.mismatches) > 0 {
		for _, m := range report.mismatches {
			logger.Error("projection mismatch",
				slog.String("table", m.table),
				slog.Int64("live_count", m.liveCount),
				slog.Int64("rebuilt_count", m.rebuiltCount),
				slog.String("live_hash", m.liveHash),
				slog.String("rebuilt_hash", m.rebuiltHash),
			)
		}
		return fmt.Errorf("rebuild: %d projection table(s) diverged from event-log replay", len(report.mismatches))
	}
	return nil
}

type tableMismatch struct {
	table        string
	liveCount    int64
	rebuiltCount int64
	liveHash     string
	rebuiltHash  string
}

type rebuildReport struct {
	eventsReplayed int
	mismatches     []tableMismatch
}

func rebuildAndDiff(ctx context.Context, pool *pgxpool.Pool, registry *projections.Registry, schema string, logger *slog.Logger, verbose bool) (rebuildReport, error) {
	// One transaction owns the whole rebuild. Rolling back at the end
	// guarantees the sandbox schema and any side effects vanish even on
	// crash mid-fold; explicit DROP at the end is belt+suspenders for the
	// rare case where the caller chose to commit.
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return rebuildReport{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Pin timezone so that timestamptz->text comparisons in the content
	// hashes are computed identically on both sides regardless of the
	// session default the connection happened to start with.
	if _, err := tx.Exec(ctx, `SET LOCAL TIME ZONE 'UTC'`); err != nil {
		return rebuildReport{}, fmt.Errorf("rebuild: pin UTC: %w", err)
	}

	// Lock events in SHARE mode so concurrent api/worker writes can't
	// append between the fold and the diff. SHARE allows other readers
	// (including ourselves) but blocks ROW EXCLUSIVE, which is what
	// INSERT acquires.
	if _, err := tx.Exec(ctx, `LOCK TABLE public.events IN SHARE MODE`); err != nil {
		return rebuildReport{}, fmt.Errorf("rebuild: lock events: %w", err)
	}

	if err := createSandboxSchema(ctx, tx, schema); err != nil {
		return rebuildReport{}, err
	}

	// Redirect unqualified table writes from projectors into the sandbox.
	// Projection writers in internal/{auth,inbox,workitems,idempotency,
	// signals} all use unqualified table names; if any future projector
	// hard-codes `public.tokens` etc. it will silently bypass the
	// sandbox and the rebuild will be wrong. The diff would then surface
	// a phantom "tables match exactly" because we'd be writing into the
	// real tables. Defending against that requires reviewing new
	// projectors; documenting it here so it isn't a surprise.
	if _, err := tx.Exec(ctx, fmt.Sprintf(`SET LOCAL search_path TO %s, public`, quoteIdent(schema))); err != nil {
		return rebuildReport{}, fmt.Errorf("rebuild: set search_path: %w", err)
	}

	count, err := foldEvents(ctx, tx, registry, logger)
	if err != nil {
		return rebuildReport{}, err
	}

	mismatches, err := diffProjections(ctx, tx, schema, logger, verbose)
	if err != nil {
		return rebuildReport{}, err
	}

	if _, err := tx.Exec(ctx, fmt.Sprintf(`DROP SCHEMA %s CASCADE`, quoteIdent(schema))); err != nil {
		return rebuildReport{}, fmt.Errorf("rebuild: drop sandbox: %w", err)
	}
	// Rollback on the deferred path is intentional even on success: the
	// rebuild produces no durable artifacts, only a verdict.
	return rebuildReport{eventsReplayed: count, mismatches: mismatches}, nil
}

func createSandboxSchema(ctx context.Context, tx pgx.Tx, schema string) error {
	qSchema := quoteIdent(schema)
	if _, err := tx.Exec(ctx, fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, qSchema)); err != nil {
		return fmt.Errorf("rebuild: drop pre-existing sandbox: %w", err)
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %s`, qSchema)); err != nil {
		return fmt.Errorf("rebuild: create sandbox: %w", err)
	}
	for _, t := range projectionTables {
		// LIKE INCLUDING ALL copies columns, defaults, NOT NULL, CHECK,
		// indexes and identity, but deliberately NOT foreign keys. We
		// don't recreate the FKs in the sandbox because the projectors
		// are the authority on what should land where; an FK would
		// catch a real projector bug only by aborting the rebuild
		// before the diff could explain the divergence.
		ddl := fmt.Sprintf(`CREATE TABLE %s.%s (LIKE public.%s INCLUDING ALL)`, qSchema, t, t)
		if _, err := tx.Exec(ctx, ddl); err != nil {
			return fmt.Errorf("rebuild: clone schema for %s: %w", t, err)
		}
	}
	for _, t := range rebuildScratchTables {
		ddl := fmt.Sprintf(`CREATE TABLE %s.%s (LIKE public.%s INCLUDING ALL)`, qSchema, t, t)
		if _, err := tx.Exec(ctx, ddl); err != nil {
			return fmt.Errorf("rebuild: clone scratch schema for %s: %w", t, err)
		}
	}
	return nil
}

func foldEvents(ctx context.Context, tx pgx.Tx, registry *projections.Registry, logger *slog.Logger) (int, error) {
	// Materialize all events before applying any. Two reasons in
	// tension; both push the same way:
	//
	//  1. pgx serializes operations on a single connection. Calling
	//     tx.Exec from inside a registry.Apply while a tx.Query Rows
	//     iterator is still open returns `conn busy`. The only way to
	//     interleave reads and writes on the same tx is to finish one
	//     before starting the other.
	//  2. Memory cost is bounded by the event log size, which v0 keeps
	//     small by design (every projection is a deterministic fold of
	//     events; the log is the system). Substantially larger logs
	//     would warrant DECLARE CURSOR ... FETCH paging or a separate
	//     read connection in autocommit mode; tracked in the v1
	//     substrate as part of "Worker process and job queue".
	//
	// events.seq is the sequencing primitive the live feed and SSE paths use.
	// Rebuild must follow the same monotonic insert order; occurred_at is a
	// timestamp, and deterministic event ids are content-addressed rather than
	// chronological.
	rows, err := tx.Query(ctx, `
		SELECT id, seq, occurred_at, actor_token_id, source, subject_kind, subject_id, kind, payload
		FROM public.events
		ORDER BY seq ASC
	`)
	if err != nil {
		return 0, fmt.Errorf("rebuild: scan events: %w", err)
	}

	var events []domain.Event
	for rows.Next() {
		var (
			ev          domain.Event
			payloadJSON []byte
			source      string
		)
		if err := rows.Scan(&ev.ID, &ev.Seq, &ev.OccurredAt, &ev.ActorTokenID, &source, &ev.SubjectKind, &ev.SubjectID, &ev.Kind, &payloadJSON); err != nil {
			rows.Close()
			return 0, fmt.Errorf("rebuild: scan event row: %w", err)
		}
		ev.Source = domain.Source(source)
		// Projectors round-trip the payload through json.Marshal/
		// Unmarshal, so unmarshalling once here is harmless and gives
		// every projector the same generic shape (map[string]any) it
		// would see during a normal write — even though the in-process
		// write path passes the typed value directly. The two paths
		// converge on identical projection rows because every
		// projector's typed view is built by re-marshaling.
		var payload any
		if len(payloadJSON) > 0 {
			if err := json.Unmarshal(payloadJSON, &payload); err != nil {
				rows.Close()
				return 0, fmt.Errorf("rebuild: decode payload for event %s: %w", ev.ID, err)
			}
		}
		ev.Payload = payload
		events = append(events, ev)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, fmt.Errorf("rebuild: stream events: %w", err)
	}
	rows.Close()

	for _, ev := range events {
		if err := registry.Apply(ctx, tx, ev); err != nil {
			return 0, fmt.Errorf("rebuild: replay %s/%s: %w", ev.Kind, ev.ID, err)
		}
	}
	logger.Info("rebuild: events folded", slog.Int("count", len(events)))
	return len(events), nil
}

func diffProjections(ctx context.Context, tx pgx.Tx, schema string, logger *slog.Logger, verbose bool) ([]tableMismatch, error) {
	var mismatches []tableMismatch
	for _, t := range projectionTables {
		liveCount, liveHash, err := tableSignature(ctx, tx, "public", t)
		if err != nil {
			return nil, fmt.Errorf("rebuild: signature live %s: %w", t, err)
		}
		rebuiltCount, rebuiltHash, err := tableSignature(ctx, tx, schema, t)
		if err != nil {
			return nil, fmt.Errorf("rebuild: signature rebuilt %s: %w", t, err)
		}
		if verbose {
			logger.Info("rebuild: table signature",
				slog.String("table", t),
				slog.Int64("live_count", liveCount),
				slog.Int64("rebuilt_count", rebuiltCount),
				slog.String("live_hash", liveHash),
				slog.String("rebuilt_hash", rebuiltHash),
			)
		}
		if liveCount != rebuiltCount || liveHash != rebuiltHash {
			mismatches = append(mismatches, tableMismatch{
				table:        t,
				liveCount:    liveCount,
				rebuiltCount: rebuiltCount,
				liveHash:     liveHash,
				rebuiltHash:  rebuiltHash,
			})
		}
	}
	return mismatches, nil
}

// tableSignature returns the row count and a content hash that is
// stable under physical row reordering. The hash is md5 of the sorted
// concatenation of per-row md5(t::text) signatures; sort-then-hash
// makes the result independent of insertion order. Any byte-level
// difference in any column (including JSONB normalization, NULLs,
// timestamps) shows up as a different hash.
func tableSignature(ctx context.Context, tx pgx.Tx, schema, table string) (int64, string, error) {
	q := fmt.Sprintf(`
		SELECT
			COALESCE(COUNT(*), 0)::bigint,
			COALESCE(md5(string_agg(sig, ',' ORDER BY sig)), '') AS hash
		FROM (
			SELECT md5(t::text) AS sig FROM %s.%s t
		) s
	`, quoteIdent(schema), quoteIdent(table))
	var (
		count int64
		hash  string
	)
	if err := tx.QueryRow(ctx, q).Scan(&count, &hash); err != nil {
		return 0, "", err
	}
	return count, hash, nil
}

// looksLikeIdentifier vets a user-supplied schema name. We intentionally
// permit only a-zA-Z0-9_ to keep the test cheap; legitimate schema
// names inside a Postgres deployment dedicated to meristem never need
// punctuation.
func looksLikeIdentifier(s string) bool {
	if s == "" || len(s) > 63 {
		return false
	}
	for i, r := range s {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r == '_':
		case i > 0 && r >= '0' && r <= '9':
		default:
			return false
		}
	}
	return true
}

// quoteIdent doubles any embedded quote and wraps the identifier in
// double quotes. Combined with looksLikeIdentifier above, this is
// belt+suspenders against identifier injection.
func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

func rebuildUsage(w io.Writer) {
	fmt.Fprint(w, `usage:
  meristem rebuild [--schema=NAME] [--verbose]

Folds public.events through every registered projector into a sandbox
schema and diffs the result against the live projection tables. The
sandbox is dropped on completion; live data is never modified.

Exit status:
  0 on full match
  1 on any mismatch (table-level rebuilt vs live divergence)
  non-zero on operational error
`)
}
