package pgtest

import (
	"context"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/storage"
)

const (
	EnvIntegrationEnabled = "MERISTEM_INTEGRATION"
	EnvTestDatabaseURL    = "MERISTEM_TEST_DATABASE_URL"
)

// NewPool returns a fresh Postgres database for an integration test and drops
// it during test cleanup. Local runs opt in with MERISTEM_INTEGRATION=1; CI
// sets that flag and provides MERISTEM_TEST_DATABASE_URL.
func NewPool(t testing.TB, namePrefix string) *pgxpool.Pool {
	t.Helper()

	baseURL := os.Getenv(EnvTestDatabaseURL)
	if baseURL == "" {
		if os.Getenv(EnvIntegrationEnabled) != "1" {
			t.Skipf("set %s=1 and %s (or %s) to run Postgres integration tests", EnvIntegrationEnabled, storage.EnvDatabaseURL, EnvTestDatabaseURL)
		}
		baseURL = os.Getenv(storage.EnvDatabaseURL)
	}
	if baseURL == "" {
		t.Skipf("%s is required for integration tests", storage.EnvDatabaseURL)
	}

	parsed, err := url.Parse(baseURL)
	if err != nil {
		t.Fatalf("parse database url: %v", err)
	}
	if parsed.Scheme != "postgres" && parsed.Scheme != "postgresql" {
		t.Fatalf("integration tests require postgres URL DSN, got scheme %q", parsed.Scheme)
	}

	prefix := strings.Trim(namePrefix, "_")
	if prefix == "" {
		prefix = "meristem_itest"
	}
	dbName := prefix + "_" + strings.ReplaceAll(uuid.NewString(), "-", "")
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
	if _, err := admin.Exec(ctx, `CREATE DATABASE `+quoteIdentifier(dbName)); err != nil {
		admin.Close()
		t.Fatalf("create temp database %s: %v", dbName, err)
	}
	admin.Close()

	pool, err := storage.Open(ctx, storage.Config{
		DatabaseURL:    testURL.String(),
		MaxConns:       4,
		MinConns:       1,
		ConnectTimeout: 10 * time.Second,
	})
	if err != nil {
		dropDatabase(t, adminURL.String(), dbName)
		t.Fatalf("open temp database: %v", err)
	}

	t.Cleanup(func() {
		pool.Close()
		dropDatabase(t, adminURL.String(), dbName)
	})
	return pool
}

func dropDatabase(t testing.TB, adminDSN string, dbName string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, adminDSN)
	if err != nil {
		t.Fatalf("open admin database for cleanup: %v", err)
	}
	defer admin.Close()
	if _, err := admin.Exec(ctx, `DROP DATABASE IF EXISTS `+quoteIdentifier(dbName)+` WITH (FORCE)`); err != nil {
		t.Fatalf("drop temp database %s: %v", dbName, err)
	}
}

func quoteIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}
