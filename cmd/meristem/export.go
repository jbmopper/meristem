package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"

	corpus "github.com/jbmopper/meristem/internal/export"
	"github.com/jbmopper/meristem/internal/storage"
)

// `meristem export` writes the publishable corpus to stdout: a
// deterministic, privacy-scrubbed JSONL fold over the events table per
// docs/refresh-requirements.md R8. Raw database dumps stay private; this is
// the projection safe to share. The command is read-only; an export run
// leaves no trace in the log it exports.
func runExport(ctx context.Context, logger *slog.Logger, args []string) error {
	fs := flag.NewFlagSet("export", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	validate := fs.Bool("validate", false, "validate the scrubbed corpus against private token names/message bodies and print a JSON report")
	if err := fs.Parse(args); err != nil {
		exportUsage(os.Stderr)
		return err
	}
	if fs.NArg() > 0 {
		exportUsage(os.Stderr)
		return fmt.Errorf("export: unexpected arguments %v", fs.Args())
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

	if *validate {
		report, err := corpus.Validate(ctx, pool)
		if encErr := json.NewEncoder(os.Stdout).Encode(report); encErr != nil && err == nil {
			err = encErr
		}
		if err != nil {
			return err
		}
		logger.Info("corpus validation complete",
			slog.Int("events_exported", report.EventsExported),
			slog.Int("lines_checked", report.LinesChecked),
			slog.Int("token_names_checked", report.TokenNamesChecked),
			slog.Int("message_bodies_checked", report.MessageBodiesChecked),
		)
		return nil
	}

	count, err := corpus.Run(ctx, pool, os.Stdout)
	if err != nil {
		return err
	}
	logger.Info("corpus exported", slog.Int("events", count))
	return nil
}

func exportUsage(w io.Writer) {
	fmt.Fprint(w, `usage:
  meristem export > corpus.jsonl
  meristem export --validate

Writes the publishable R8 corpus to stdout as scrubbed JSONL.

  --validate  do not write corpus JSONL; run the exporter in memory, compare
              the result against private token names and message.captured
              bodies still in the database, and print a non-sensitive JSON
              validation report.
`)
}
