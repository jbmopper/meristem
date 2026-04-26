package storage

import (
	"testing"

	"github.com/jbmopper/meristem/migrations"
)

// TestEmbeddedMigrationsLoad validates that the embedded migration FS is
// non-empty, every filename matches the expected pattern, and every up file
// has a matching down file. It does not require a live database — the goal
// is to catch parser regressions and "I forgot to commit the down file"
// before they hit a deployment.
func TestEmbeddedMigrationsLoad(t *testing.T) {
	files, err := loadMigrationFiles(migrations.FS)
	if err != nil {
		t.Fatalf("loadMigrationFiles: %v", err)
	}
	if len(files) == 0 {
		t.Fatal("expected at least one migration in the embedded FS")
	}

	ups := map[int64]bool{}
	downs := map[int64]bool{}
	for _, f := range files {
		if f.Up {
			ups[f.Version] = true
		} else {
			downs[f.Version] = true
		}
	}
	for v := range ups {
		if !downs[v] {
			t.Errorf("migration %d has up but no down", v)
		}
	}
	for v := range downs {
		if !ups[v] {
			t.Errorf("migration %d has down but no up", v)
		}
	}

	// The first migration must be 1; we numerically order migrations and
	// rely on dense numbering for human readability.
	if !ups[1] {
		t.Error("expected migration 0001 (init) to be present")
	}
}
