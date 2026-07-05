package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/jbmopper/meristem/internal/export"
	"github.com/jbmopper/meristem/internal/storage"
)

// `meristem export` writes the publishable corpus to stdout: a
// deterministic, privacy-scrubbed JSONL fold over the events table per
// docs/refresh-requirements.md R8. Raw database dumps stay private; this is
// the projection safe to share. The command is read-only; an export run
// leaves no trace in the log it exports.
func runExport(ctx context.Context, logger *slog.Logger, args []string) error {
	if len(args) > 0 {
		fmt.Fprintln(os.Stderr, "usage: meristem export > corpus.jsonl")
		return fmt.Errorf("export: unexpected arguments %v", args)
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

	count, err := export.Run(ctx, pool, os.Stdout)
	if err != nil {
		return err
	}
	logger.Info("corpus exported", slog.Int("events", count))
	return nil
}
