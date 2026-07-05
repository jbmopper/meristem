package export

import (
	"bytes"
	"context"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/inbox"
	"github.com/jbmopper/meristem/internal/storage"
)

const (
	envIntegrationEnabled = "MERISTEM_INTEGRATION"
	envTestDatabaseURL    = "MERISTEM_TEST_DATABASE_URL"
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
	for _, forbidden := range []string{tokenName, inboxText, `"kind":"token.created"`, `"kind":"message.captured"`, `"kind":"idempotency.recorded"`} {
		if strings.Contains(out, forbidden) {
			t.Errorf("corpus contains forbidden content: %s", forbidden)
		}
	}
	if !strings.Contains(out, `"kind":"work_item.created"`) {
		t.Error("corpus missing the allowlisted work_item.created event")
	}
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		if strings.Contains(line, `"kind"`) && !containsAllowlistedKind(line) {
			t.Errorf("corpus line carries non-allowlisted kind: %.120s", line)
		}
	}
}

func containsAllowlistedKind(line string) bool {
	for kind := range KindAllowlist {
		if strings.Contains(line, `"kind":"`+kind+`"`) {
			return true
		}
	}
	return false
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
	baseURL := os.Getenv(envTestDatabaseURL)
	if baseURL == "" {
		if os.Getenv(envIntegrationEnabled) != "1" {
			t.Skipf("set %s=1 and %s to run Postgres integration tests", envIntegrationEnabled, envTestDatabaseURL)
		}
		baseURL = os.Getenv(storage.EnvDatabaseURL)
	}
	if baseURL == "" {
		t.Skipf("%s is required for integration tests", envTestDatabaseURL)
	}
	parsed, err := url.Parse(baseURL)
	if err != nil {
		t.Fatalf("parse database url: %v", err)
	}
	dbName := "meristem_export_itest_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	adminURL := *parsed
	adminURL.Path = "/postgres"
	testURL := *parsed
	testURL.Path = "/" + dbName

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, adminURL.String())
	if err != nil {
		t.Fatalf("open admin database: %v", err)
	}
	if _, err := admin.Exec(ctx, `CREATE DATABASE "`+dbName+`"`); err != nil {
		admin.Close()
		t.Fatalf("create temp database: %v", err)
	}
	admin.Close()
	pool, err := storage.Open(ctx, storage.Config{DatabaseURL: testURL.String(), MaxConns: 4, MinConns: 1, ConnectTimeout: 10 * time.Second})
	if err != nil {
		t.Fatalf("open temp database: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}
