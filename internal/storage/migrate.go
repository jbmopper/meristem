package storage

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/migrations"
)

// MigrationDirection selects which set of files to apply. v0 only exposes Up
// from the binary; Down is implemented for completeness and used by tests.
type MigrationDirection int

const (
	DirectionUp MigrationDirection = iota
	DirectionDown

	// humanReviewGateBoundaryMigration is the one migration whose application
	// records the event-log position at which its projector invariant became
	// authoritative. Historical events at or below that position must retain
	// their original fold semantics during rebuild.
	humanReviewGateBoundaryMigration int64 = 40
)

// migrationFilenamePattern matches "0001_name.up.sql" or "0001_name.down.sql".
// Versions are 4-digit zero-padded so lexicographic and numeric order agree,
// but we still parse to int for safety.
var migrationFilenamePattern = regexp.MustCompile(`^(\d+)_([a-z0-9_]+)\.(up|down)\.sql$`)

type migrationFile struct {
	Version  int64
	Name     string
	Filename string
	Up       bool
}

// Migrate applies all pending Up migrations from the embedded migrations FS.
// Each migration runs in its own transaction; partial application across
// files is therefore impossible. Re-running Migrate after a successful run
// is a no-op (idempotent — the substrate property the rest of the system
// relies on).
func Migrate(ctx context.Context, pool *pgxpool.Pool, logger *slog.Logger) error {
	return MigrateWithCheck(ctx, pool, logger, nil)
}

// MigrateWithCheck applies all pending Up migrations while re-running check
// at every durable mutation boundary. A failed check prevents a new migration
// transaction from starting or rolls back the transaction before commit.
func MigrateWithCheck(ctx context.Context, pool *pgxpool.Pool, logger *slog.Logger, check func() error) error {
	return migrate(ctx, pool, logger, migrations.FS, DirectionUp, check)
}

// MigrateDown rolls back the most recently applied migration. Intended for
// tests and operator recovery, not for the normal startup path.
func MigrateDown(ctx context.Context, pool *pgxpool.Pool, logger *slog.Logger) error {
	return MigrateDownWithCheck(ctx, pool, logger, nil)
}

// MigrateDownWithCheck rolls back the most recently applied migration while
// enforcing check at the same durable mutation boundaries as MigrateWithCheck.
func MigrateDownWithCheck(ctx context.Context, pool *pgxpool.Pool, logger *slog.Logger, check func() error) error {
	return migrate(ctx, pool, logger, migrations.FS, DirectionDown, check)
}

func migrate(
	ctx context.Context,
	pool *pgxpool.Pool,
	logger *slog.Logger,
	source fs.FS,
	dir MigrationDirection,
	check func() error,
) error {
	if logger == nil {
		logger = slog.Default()
	}
	if err := runMigrationCheck(check); err != nil {
		return err
	}
	if err := ensureSchemaMigrationsTable(ctx, pool); err != nil {
		return err
	}

	files, err := loadMigrationFiles(source)
	if err != nil {
		return err
	}
	applied, err := loadAppliedVersions(ctx, pool)
	if err != nil {
		return err
	}

	switch dir {
	case DirectionUp:
		return runUp(ctx, pool, logger, source, files, applied, check)
	case DirectionDown:
		return runDown(ctx, pool, logger, source, files, applied, check)
	default:
		return fmt.Errorf("storage: unknown migration direction %d", dir)
	}
}

func runUp(
	ctx context.Context,
	pool *pgxpool.Pool,
	logger *slog.Logger,
	source fs.FS,
	files []migrationFile,
	applied map[int64]bool,
	check func() error,
) error {
	pending := []migrationFile{}
	for _, f := range files {
		if !f.Up || applied[f.Version] {
			continue
		}
		pending = append(pending, f)
	}
	if len(pending) == 0 {
		logger.Info("migrations up to date")
		return nil
	}
	for _, f := range pending {
		if err := applyMigration(ctx, pool, logger, source, f, true, check); err != nil {
			return err
		}
	}
	return nil
}

func runDown(
	ctx context.Context,
	pool *pgxpool.Pool,
	logger *slog.Logger,
	source fs.FS,
	files []migrationFile,
	applied map[int64]bool,
	check func() error,
) error {
	// Find the highest applied version that has a matching down file, then
	// roll exactly that one back. Rolling back the whole world from a single
	// command is too easy to do by accident.
	var latest *migrationFile
	for i := range files {
		f := files[i]
		if f.Up || !applied[f.Version] {
			continue
		}
		if latest == nil || f.Version > latest.Version {
			latest = &f
		}
	}
	if latest == nil {
		logger.Info("nothing to roll back")
		return nil
	}
	return applyMigration(ctx, pool, logger, source, *latest, false, check)
}

func applyMigration(
	ctx context.Context,
	pool *pgxpool.Pool,
	logger *slog.Logger,
	source fs.FS,
	f migrationFile,
	up bool,
	check func() error,
) error {
	body, err := fs.ReadFile(source, f.Filename)
	if err != nil {
		return fmt.Errorf("storage: read %s: %w", f.Filename, err)
	}

	// Each migration is its own transaction. If a future migration needs
	// CREATE INDEX CONCURRENTLY (which cannot run inside a tx) we'll add an
	// opt-out marker; v0 has no such case.
	if err := runMigrationCheck(check); err != nil {
		return err
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("storage: begin tx for %s: %w", f.Filename, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, string(body)); err != nil {
		return fmt.Errorf("storage: apply %s: %w", f.Filename, err)
	}

	if up {
		if f.Version == humanReviewGateBoundaryMigration {
			// Include every event committed by an older writer before activation.
			// INSERT takes ROW EXCLUSIVE, so SHARE waits for in-flight appends and
			// prevents a new one from straddling the boundary transaction.
			if _, err = tx.Exec(ctx, `LOCK TABLE events IN SHARE MODE`); err != nil {
				return fmt.Errorf("storage: lock events for %s boundary: %w", f.Filename, err)
			}
			_, err = tx.Exec(ctx, `
				INSERT INTO schema_migrations (version, name, event_seq_boundary)
				SELECT $1, $2, COALESCE(max(seq), 0)
				FROM events
			`, f.Version, f.Name)
		} else {
			_, err = tx.Exec(ctx,
				`INSERT INTO schema_migrations (version, name) VALUES ($1, $2)`,
				f.Version, f.Name)
		}
	} else {
		_, err = tx.Exec(ctx,
			`DELETE FROM schema_migrations WHERE version = $1`,
			f.Version)
	}
	if err != nil {
		return fmt.Errorf("storage: record %s: %w", f.Filename, err)
	}

	if err := runMigrationCheck(check); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("storage: commit %s: %w", f.Filename, err)
	}

	direction := "up"
	if !up {
		direction = "down"
	}
	logger.Info("migration applied",
		slog.Int64("version", f.Version),
		slog.String("name", f.Name),
		slog.String("direction", direction),
	)
	return nil
}

func runMigrationCheck(check func() error) error {
	if check == nil {
		return nil
	}
	if err := check(); err != nil {
		return fmt.Errorf("storage: migration guard: %w", err)
	}
	return nil
}

func ensureSchemaMigrationsTable(ctx context.Context, pool *pgxpool.Pool) error {
	const ddl = `
CREATE TABLE IF NOT EXISTS schema_migrations (
    version    BIGINT      PRIMARY KEY,
    name       TEXT        NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
)`
	if _, err := pool.Exec(ctx, ddl); err != nil {
		return fmt.Errorf("storage: ensure schema_migrations: %w", err)
	}
	return nil
}

func loadAppliedVersions(ctx context.Context, pool *pgxpool.Pool) (map[int64]bool, error) {
	rows, err := pool.Query(ctx, `SELECT version FROM schema_migrations`)
	if err != nil {
		return nil, fmt.Errorf("storage: load applied versions: %w", err)
	}
	defer rows.Close()
	out := map[int64]bool{}
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			return nil, fmt.Errorf("storage: scan version: %w", err)
		}
		out[v] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("storage: iterate versions: %w", err)
	}
	return out, nil
}

func loadMigrationFiles(source fs.FS) ([]migrationFile, error) {
	entries, err := fs.ReadDir(source, ".")
	if err != nil {
		return nil, fmt.Errorf("storage: read migrations dir: %w", err)
	}
	files := make([]migrationFile, 0, len(entries))
	seen := map[string]bool{}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasSuffix(name, ".sql") {
			continue
		}
		m := migrationFilenamePattern.FindStringSubmatch(name)
		if m == nil {
			return nil, fmt.Errorf("storage: malformed migration filename: %s", name)
		}
		version, err := strconv.ParseInt(m[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("storage: parse version %q: %w", m[1], err)
		}
		key := fmt.Sprintf("%d-%s", version, m[3])
		if seen[key] {
			return nil, fmt.Errorf("storage: duplicate migration %s", name)
		}
		seen[key] = true
		files = append(files, migrationFile{
			Version:  version,
			Name:     m[2],
			Filename: name,
			Up:       m[3] == "up",
		})
	}
	if len(files) == 0 {
		return nil, errors.New("storage: no migrations found")
	}
	sort.Slice(files, func(i, j int) bool {
		if files[i].Version != files[j].Version {
			return files[i].Version < files[j].Version
		}
		return files[i].Up && !files[j].Up
	})
	return files, nil
}
