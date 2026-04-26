// Package storage owns the Postgres pool and the migration runner.
//
// Per the spec, Postgres is the system: every durable thing — state, queues,
// audit — lives here. The pool is process-wide and is the only thing the
// rest of the codebase should reach for when it needs the database.
package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// EnvDatabaseURL is the environment variable meristem reads to discover its
// Postgres DSN. The variable is namespaced so it cannot collide with other
// processes that happen to share the same shell (e.g. psql, other tools that
// inspect the bare DATABASE_URL).
const EnvDatabaseURL = "MERISTEM_DATABASE_URL"

// Config holds the knobs we expose to callers. Sensible defaults mean the
// zero value is usable in tests; callers that want explicit control set
// fields directly.
type Config struct {
	DatabaseURL     string
	MaxConns        int32
	MinConns        int32
	MaxConnLifetime time.Duration
	MaxConnIdleTime time.Duration
	ConnectTimeout  time.Duration
}

// LoadConfigFromEnv builds a Config from the environment, applying defaults
// for anything not set. It returns an error only when a required value is
// missing — connectivity is verified by Open, not here.
func LoadConfigFromEnv() (Config, error) {
	dsn := os.Getenv(EnvDatabaseURL)
	if dsn == "" {
		return Config{}, fmt.Errorf("%s is not set", EnvDatabaseURL)
	}
	return Config{
		DatabaseURL:     dsn,
		MaxConns:        10,
		MinConns:        1,
		MaxConnLifetime: time.Hour,
		MaxConnIdleTime: 30 * time.Minute,
		ConnectTimeout:  10 * time.Second,
	}, nil
}

// Open returns a ready-to-use connection pool. It performs an initial Ping
// so callers fail fast on bad credentials instead of at first query.
func Open(ctx context.Context, cfg Config) (*pgxpool.Pool, error) {
	if cfg.DatabaseURL == "" {
		return nil, errors.New("storage: DatabaseURL is required")
	}

	poolCfg, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("storage: parse dsn: %w", err)
	}
	if cfg.MaxConns > 0 {
		poolCfg.MaxConns = cfg.MaxConns
	}
	if cfg.MinConns > 0 {
		poolCfg.MinConns = cfg.MinConns
	}
	if cfg.MaxConnLifetime > 0 {
		poolCfg.MaxConnLifetime = cfg.MaxConnLifetime
	}
	if cfg.MaxConnIdleTime > 0 {
		poolCfg.MaxConnIdleTime = cfg.MaxConnIdleTime
	}

	pingCtx := ctx
	if cfg.ConnectTimeout > 0 {
		var cancel context.CancelFunc
		pingCtx, cancel = context.WithTimeout(ctx, cfg.ConnectTimeout)
		defer cancel()
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("storage: open pool: %w", err)
	}
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("storage: ping: %w", err)
	}
	return pool, nil
}
