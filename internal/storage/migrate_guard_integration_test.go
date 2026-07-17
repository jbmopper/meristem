package storage_test

import (
	"context"
	"errors"
	"testing"

	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/testutil/pgtest"
)

func TestMigrateWithCheckRollsBackWhenGuardChangesBeforeCommit(t *testing.T) {
	pool := pgtest.NewPool(t, "migration_guard_commit")
	guardErr := errors.New("reviewed pin changed")
	checks := 0

	err := storage.MigrateWithCheck(context.Background(), pool, nil, func() error {
		checks++
		if checks == 3 {
			return guardErr
		}
		return nil
	})
	if !errors.Is(err, guardErr) {
		t.Fatalf("MigrateWithCheck error = %v, want wrapped guard error", err)
	}

	var applied int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM schema_migrations`).Scan(&applied); err != nil {
		t.Fatalf("count schema migrations: %v", err)
	}
	if applied != 0 {
		t.Fatalf("applied migrations = %d, want 0 after guarded rollback", applied)
	}
}

func TestMigrateWithCheckStopsBeforeNextMigration(t *testing.T) {
	pool := pgtest.NewPool(t, "migration_guard_between")
	guardErr := errors.New("reviewed pin changed")
	checks := 0

	err := storage.MigrateWithCheck(context.Background(), pool, nil, func() error {
		checks++
		if checks == 4 {
			return guardErr
		}
		return nil
	})
	if !errors.Is(err, guardErr) {
		t.Fatalf("MigrateWithCheck error = %v, want wrapped guard error", err)
	}

	var applied []int64
	rows, err := pool.Query(context.Background(), `SELECT version FROM schema_migrations ORDER BY version`)
	if err != nil {
		t.Fatalf("query schema migrations: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var version int64
		if err := rows.Scan(&version); err != nil {
			t.Fatalf("scan migration version: %v", err)
		}
		applied = append(applied, version)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate migration versions: %v", err)
	}
	if len(applied) != 1 || applied[0] != 1 {
		t.Fatalf("applied migrations = %v, want [1]", applied)
	}
}
