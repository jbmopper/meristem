// `meristem worker --once` runs a single bounded-patience scan: it reads
// every non-terminal work_item, runs checklist convergence for running
// work_items with suggested checks, compares remaining dwell time against the
// per-state budget, appends one patience.breached event per observed breach,
// and routes each breached state epoch to a human escalation unless it is
// already awaiting human review.
//
// The intent of this slice is to land the kernel and the on-the-wire
// signal: by adding the worker subcommand and the patience.breached event
// kind, the running system can now observe bounded-patience invariant
// violations and reconcile checklist-declared convergence. The daemon loop is
// a subsequent slice.
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
	"github.com/jbmopper/meristem/internal/policyprofile"
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
	// --budget=DURATION overrides the per-state budgets with a single
	// uniform budget applied to every non-terminal state. The intent is
	// operator/CI verification ("show me what the worker sees right now
	// against an aggressively tight budget"), not production policy.
	// Production budgets come from the active policy profile.
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

	// Budgets come from the active policy profile (bring-up vs steady),
	// resolved from the event-sourced projection; an un-switched or
	// pre-0014 database resolves to steady, which equals the previous
	// hardcoded defaults. --budget still overrides everything for
	// operator/CI verification runs.
	profileSvc := policyprofile.NewService(pool, writer)
	active, err := profileSvc.Active(ctx)
	if err != nil {
		return fmt.Errorf("worker: resolve active policy profile: %w", err)
	}
	budgets := worker.Budgets{ByState: active.Policy.PatienceBudgets}
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
		slog.Int("patience_escalations_requested", result.PatienceEscalationsRequested),
		slog.Int("patience_escalations_already_requested", result.PatienceEscalationsAlreadyRequested),
		slog.Int("patience_escalations_skipped_awaiting_human", result.PatienceEscalationsSkippedAwaitingHuman),
		slog.Int("scribe_candidates", result.ScribeCandidatesScanned),
		slog.Int("scribe_children_spawned", result.ScribeChildrenSpawned),
		slog.Int("scribe_children_already_present", result.ScribeChildrenAlreadyPresent),
		slog.Int("dispatch_candidates", result.DispatchCandidatesScanned),
		slog.Int("dispatch_requested", result.DispatchesRequested),
		slog.Int("dispatch_already_requested", result.DispatchesAlreadyRequested),
		slog.Int("dispatch_skipped_missing_cultivar", result.DispatchesSkippedMissingCultivar),
		slog.Int("convergence_candidates", result.ConvergenceCandidatesScanned),
		slog.Int("convergence_verdicts_recorded", result.ConvergenceVerdictsRecorded),
		slog.Int("convergence_verdicts_already_recorded", result.ConvergenceVerdictsAlreadyRecorded),
		slog.Int("convergence_stale_inputs_skipped", result.ConvergenceStaleInputsSkipped),
		slog.Int("convergence_accepts", result.ConvergenceAccepts),
		slog.Int("convergence_retries", result.ConvergenceRetries),
		slog.Int("convergence_escalations", result.ConvergenceEscalations),
	)
	fmt.Fprintln(os.Stdout, formatWorkerOnceResult(result))
	return nil
}

func formatWorkerOnceResult(result worker.Result) string {
	return fmt.Sprintf("worker --once: scanned=%d emitted=%d already_recorded=%d patience_escalations=%d patience_escalations_already_requested=%d patience_escalations_skipped_awaiting_human=%d scribe_candidates=%d scribe_children_spawned=%d scribe_children_already_present=%d dispatch_candidates=%d dispatch_requested=%d dispatch_already_requested=%d dispatch_skipped_missing_cultivar=%d convergence_candidates=%d convergence_verdicts=%d stale_inputs_skipped=%d accepts=%d retries=%d escalations=%d",
		result.Scanned,
		result.BreachesEmitted,
		result.BreachesAlreadyRecorded,
		result.PatienceEscalationsRequested,
		result.PatienceEscalationsAlreadyRequested,
		result.PatienceEscalationsSkippedAwaitingHuman,
		result.ScribeCandidatesScanned,
		result.ScribeChildrenSpawned,
		result.ScribeChildrenAlreadyPresent,
		result.DispatchCandidatesScanned,
		result.DispatchesRequested,
		result.DispatchesAlreadyRequested,
		result.DispatchesSkippedMissingCultivar,
		result.ConvergenceCandidatesScanned,
		result.ConvergenceVerdictsRecorded+result.ConvergenceVerdictsAlreadyRecorded,
		result.ConvergenceStaleInputsSkipped,
		result.ConvergenceAccepts,
		result.ConvergenceRetries,
		result.ConvergenceEscalations,
	)
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

Runs a single bounded-patience scan. Runs checklist convergence for running
work_items with suggested_convergence_checks, then reads remaining
non-terminal work_items, compares dwell time to the per-state budget, appends
one patience.breached event per observed breach, and escalates breached state
epochs to human attention unless they are already waiting on human review.
Idempotent: re-running with the same convergence signals does not consume a new
convergence attempt.

  --budget=DURATION   override the per-state defaults with one uniform
                      budget applied to every non-terminal state. Intended
                      for operator/CI verification, not production policy.
                      Examples: --budget=1m, --budget=10s, --budget=72h.

This is the v1 substrate "Convergence loop ..." kernel. The daemon-loop
form is a subsequent slice.
`)
}
