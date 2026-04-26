// `meristem worker --once` runs a single bounded-patience scan: it reads
// every non-terminal work_item, compares dwell time against the per-state
// budget, and appends one patience.breached event per observed breach.
//
// The intent of this slice is to land the kernel and the on-the-wire
// signal: by adding the worker subcommand and the patience.breached event
// kind, the running system can now *observe* its own bounded-patience
// invariant violations, which is the prerequisite for any acting on them.
// The acting (escalation, retry, forced fail) and the daemon loop are
// next slices.
//
// Authentication mirrors `meristem seed v1`: MERISTEM_TOKEN must be a
// system-source token. The events the worker writes attribute to "system"
// regardless, but the actor_token_id field is what links the event back
// to a specific worker process for audit.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/worker"
)

func runWorker(ctx context.Context, logger *slog.Logger, args []string) error {
	if len(args) == 0 {
		workerUsage(os.Stderr)
		return fmt.Errorf("worker: missing mode (only --once is supported in this slice)")
	}

	switch args[0] {
	case "--once", "once":
		return runWorkerOnce(ctx, logger, args[1:])
	default:
		workerUsage(os.Stderr)
		return fmt.Errorf("worker: unknown mode %q (only --once is supported in this slice)", args[0])
	}
}

func runWorkerOnce(ctx context.Context, logger *slog.Logger, args []string) error {
	if _, _, err := validateStartupSafety(logger); err != nil {
		return err
	}

	fs := flag.NewFlagSet("worker --once", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	// --budget=DURATION overrides the per-state defaults with a single
	// uniform budget applied to every non-terminal state. The intent is
	// operator/CI verification ("show me what the worker sees right now
	// against an aggressively tight budget"), not production policy.
	// Production budgets stay in DefaultBudgets() until configuration
	// support lands in a later slice.
	uniformBudget := fs.Duration("budget", 0, "uniform budget applied to every non-terminal state (overrides defaults; for verification)")
	if err := fs.Parse(args); err != nil {
		workerUsage(os.Stderr)
		return err
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

	writer := app.NewEventWriter()
	authService := auth.NewService(pool, writer)

	systemTok, err := resolveWorkerSystemToken(ctx, authService)
	if err != nil {
		return err
	}

	budgets := worker.DefaultBudgets()
	if *uniformBudget > 0 {
		budgets = uniformBudgets(*uniformBudget)
	}

	w, err := worker.New(pool, writer, budgets, &systemTok.ID, nil)
	if err != nil {
		return err
	}

	result, err := w.ScanOnce(ctx)
	if err != nil {
		return err
	}

	logger.Info("worker scan complete",
		slog.Int("scanned", result.Scanned),
		slog.Int("breaches_emitted", result.BreachesEmitted),
		slog.Int("breaches_already_recorded", result.BreachesAlreadyRecorded),
	)
	fmt.Fprintf(os.Stdout, "worker --once: scanned=%d emitted=%d already_recorded=%d\n",
		result.Scanned, result.BreachesEmitted, result.BreachesAlreadyRecorded)
	return nil
}

// uniformBudgets returns a Budgets map applying d to every non-terminal
// WorkItemState. Useful for the --budget override and for any future
// "scan everything aggressively" mode.
func uniformBudgets(d time.Duration) worker.Budgets {
	return worker.Budgets{ByState: map[domain.WorkItemState]time.Duration{
		domain.WorkItemCaptured:         d,
		domain.WorkItemTriaged:          d,
		domain.WorkItemPlanned:          d,
		domain.WorkItemAwaitingApproval: d,
		domain.WorkItemRunning:          d,
		domain.WorkItemBlocked:          d,
	}}
}

// resolveWorkerSystemToken loads the bearer in MERISTEM_TOKEN and refuses
// to proceed unless it is a dedicated, non-root system token. Same policy
// as `meristem seed v1`: automation runs against the system, not via root.
func resolveWorkerSystemToken(ctx context.Context, service tokenAuthenticator) (domain.Token, error) {
	secret := os.Getenv("MERISTEM_TOKEN")
	if secret == "" {
		return domain.Token{}, fmt.Errorf("worker: MERISTEM_TOKEN with a system-source bearer is required (mint one with `meristem tokens create --source system --name worker`)")
	}
	tok, err := service.Authenticate(ctx, secret)
	if err != nil {
		return domain.Token{}, err
	}
	if tok.Source != domain.SourceSystem {
		return domain.Token{}, fmt.Errorf("worker: MERISTEM_TOKEN must be source=system, got %q (root is deliberately not accepted)", tok.Source)
	}
	if tok.IsRoot {
		return domain.Token{}, fmt.Errorf("worker: MERISTEM_TOKEN must be a dedicated system token, not root")
	}
	return tok, nil
}

func workerUsage(w io.Writer) {
	fmt.Fprint(w, `usage:
  MERISTEM_TOKEN=mrs_<system> meristem worker --once [--budget=DURATION]

Runs a single bounded-patience scan. Reads every non-terminal work_item,
compares dwell time to the per-state budget, and appends one
patience.breached event per observed breach. Idempotent: re-running with
the same observations is a no-op on the wire.

  --budget=DURATION   override the per-state defaults with one uniform
                      budget applied to every non-terminal state. Intended
                      for operator/CI verification, not production policy.
                      Examples: --budget=1m, --budget=10s, --budget=72h.

This is the v1 substrate "Convergence loop ..." kernel. The daemon-loop
form is a subsequent slice.
`)
}
