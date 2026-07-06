package export

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/inbox"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/testutil/pgtest"
)

// TestExportScrubsAndFiltersSeededDatabase is the R8 slice's convergence
// check: on a database holding token creations, a verbatim inbox capture,
// and work-item lifecycle, the corpus contains no token names, no
// message.captured bodies, no non-allowlisted kinds — and the run appends
// nothing to the log it exports.
func TestExportScrubsAndFiltersSeededDatabase(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	writer := app.NewEventWriter()

	const tokenName = "corpus-secret-token-name"
	const inboxText = "verbatim owner instruction that must never be exported"

	authSvc := auth.NewService(pool, writer)
	rootResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name: tokenName, IsRoot: true, Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create token: %v", err)
	}
	if _, err := inbox.NewService(pool, writer).CaptureText(ctx, rootResult.Token, inboxText); err != nil {
		t.Fatalf("capture: %v", err)
	}

	before := eventCount(t, pool)
	var buf bytes.Buffer
	n, err := Run(ctx, pool, &buf)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if n == 0 {
		t.Fatal("exporter emitted nothing from a seeded database")
	}
	if after := eventCount(t, pool); after != before {
		t.Fatalf("export run wrote to the log: before=%d after=%d", before, after)
	}

	out := buf.String()
	for _, forbidden := range []string{tokenName, inboxText, `"kind":"token.created"`, `"kind":"message.captured"`, `"kind":"idempotency.recorded"`, `"actor_token_id"`} {
		if strings.Contains(out, forbidden) {
			t.Errorf("corpus contains forbidden content: %s", forbidden)
		}
	}
	if !strings.Contains(out, `"kind":"work_item.created"`) {
		t.Error("corpus missing the allowlisted work_item.created event")
	}
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		var rec Record
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("decode corpus line: %v\n%s", err, line)
		}
		if !KindAllowlist[rec.Kind] {
			t.Errorf("corpus line carries non-allowlisted kind %q: %.120s", rec.Kind, line)
		}
		var raw map[string]any
		if err := json.Unmarshal([]byte(line), &raw); err != nil {
			t.Fatalf("decode raw corpus line: %v\n%s", err, line)
		}
		if _, ok := raw["actor_token_id"]; ok {
			t.Errorf("corpus line exports private actor_token_id: %.120s", line)
		}
	}

	report, err := Validate(ctx, pool)
	if err != nil {
		t.Fatalf("validate corpus: %v", err)
	}
	if !report.Valid {
		t.Fatalf("validation report not valid: %+v", report)
	}
	if report.EventsExported != n || report.LinesChecked != n {
		t.Fatalf("validation counts = events_exported:%d lines_checked:%d, want %d", report.EventsExported, report.LinesChecked, n)
	}
	if report.TokenNamesChecked == 0 || report.MessageBodiesChecked == 0 {
		t.Fatalf("validation did not check private fixtures: %+v", report)
	}
}

func eventCount(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM events`).Scan(&n); err != nil {
		t.Fatalf("count events: %v", err)
	}
	return n
}

func newIntegrationPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	return pgtest.NewPool(t, "meristem_export_itest")
}
