// `meristem worker` runs the deterministic reconciler. The default form is an
// always-on daemon loop; `meristem worker --once` runs a single bounded-patience
// scan for verification and manual operation.
//
// Each tick reads non-terminal work_items, runs checklist convergence for
// running work_items with suggested checks, compares remaining dwell time
// against the per-state budget, appends one patience.breached event per
// observed breach, and routes each breached state epoch to dispatch or human
// escalation unless it is already awaiting human review.
//
// Authentication mirrors `meristem seed v1`: MERISTEM_TOKEN must be a
// system-source token. The events the worker writes attribute to "system"
// regardless, but the actor_token_id field is what links the event back
// to a specific worker process for audit.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/crossnode"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/policyprofile"
	"github.com/jbmopper/meristem/internal/worker"
)

func runWorker(ctx context.Context, logger *slog.Logger, args []string) error {
	if len(args) > 0 {
		switch args[0] {
		case "--once", "once":
			return runWorkerOnce(ctx, logger, args[1:])
		case "daemon":
			return runWorkerDaemon(ctx, logger, args[1:])
		}
	}
	return runWorkerDaemon(ctx, logger, args)
}

type workerRuntime struct {
	pool          *pgxpool.Pool
	writer        *events.Writer
	systemTok     domain.Token
	uniformBudget time.Duration
	activeProfile policyprofile.Active
}

func newWorkerRuntime(ctx context.Context, logger *slog.Logger, uniformBudget time.Duration) (*workerRuntime, error) {
	pool, active, err := openProfileAwarePool(ctx, logger)
	if err != nil {
		return nil, err
	}

	writer := app.NewEventWriter()
	authService := auth.NewService(pool, writer)
	systemTok, err := resolveWorkerSystemToken(ctx, authService)
	if err != nil {
		pool.Close()
		return nil, err
	}

	return &workerRuntime{
		pool:          pool,
		writer:        writer,
		systemTok:     systemTok,
		uniformBudget: uniformBudget,
		activeProfile: active,
	}, nil
}

func (r *workerRuntime) Close() {
	r.pool.Close()
}

func (r *workerRuntime) ScanOnce(ctx context.Context) (worker.Result, error) {
	profileSvc := policyprofile.NewService(r.pool, r.writer)
	active, err := profileSvc.Active(ctx)
	if err != nil {
		return worker.Result{}, fmt.Errorf("worker: resolve active policy profile: %w", err)
	}
	budgets := worker.Budgets{ByState: active.Policy.PatienceBudgets}
	if r.uniformBudget > 0 {
		budgets = uniformBudgets(r.uniformBudget)
	}

	w, err := worker.New(r.pool, r.writer, budgets, &r.systemTok.ID, nil)
	if err != nil {
		return worker.Result{}, err
	}
	result, err := w.ScanOnce(ctx)
	if err != nil {
		return worker.Result{}, err
	}
	expired, err := crossnode.NewQueueService(r.pool, r.writer).ExpireDue(ctx, crossnode.ExpireDueInput{
		Now:          time.Now().UTC(),
		ActorTokenID: r.systemTok.ID,
		Source:       r.systemTok.Source,
		LocalNodeID:  os.Getenv("MERISTEM_NODE_ID"),
	})
	if err != nil {
		return worker.Result{}, fmt.Errorf("worker: expire cross-node commands: %w", err)
	}
	result.NetworkCommandsExpired = len(expired)
	return result, nil
}

func runWorkerOnce(ctx context.Context, logger *slog.Logger, args []string) error {
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
	if fs.NArg() > 0 {
		workerUsage(os.Stderr)
		return fmt.Errorf("worker --once: unexpected argument %q", fs.Arg(0))
	}

	runtime, err := newWorkerRuntime(ctx, logger, *uniformBudget)
	if err != nil {
		return err
	}
	defer runtime.Close()

	result, err := runtime.ScanOnce(ctx)
	if err != nil {
		return err
	}

	logWorkerResult(logger, "worker scan complete", runtime.systemTok, result)
	fmt.Fprintln(os.Stdout, formatWorkerOnceResult(result))
	return nil
}

func runWorkerDaemon(ctx context.Context, logger *slog.Logger, args []string) error {
	fs := flag.NewFlagSet("worker", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var intervalText string
	fs.StringVar(&intervalText, "interval", "", "duration between daemon worker ticks (defaults to active profile)")
	uniformBudget := fs.Duration("budget", 0, "uniform budget applied to every non-terminal state (overrides active profile; for verification)")
	if err := fs.Parse(args); err != nil {
		workerUsage(os.Stderr)
		return err
	}
	if fs.NArg() > 0 {
		workerUsage(os.Stderr)
		return fmt.Errorf("worker: unknown mode or argument %q", fs.Arg(0))
	}
	runtime, err := newWorkerRuntime(ctx, logger, *uniformBudget)
	if err != nil {
		return err
	}
	defer runtime.Close()

	interval, overridden, err := resolveWorkerInterval(runtime.activeProfile.Policy.WorkerTickInterval, intervalText)
	if err != nil {
		return err
	}
	logger.Info("worker daemon starting",
		slog.String("interval", interval.String()),
		slog.Bool("interval_overridden", overridden),
		slog.String("policy_profile", runtime.activeProfile.Name),
		slog.String("token_id", runtime.systemTok.ID.String()),
		slog.String("token_source", string(runtime.systemTok.Source)),
	)
	return runWorkerLoop(ctx, logger, interval, func(ctx context.Context) (worker.Result, domain.Token, error) {
		result, err := runtime.ScanOnce(ctx)
		return result, runtime.systemTok, err
	})
}

func resolveWorkerInterval(defaultInterval time.Duration, override string) (time.Duration, bool, error) {
	if defaultInterval <= 0 {
		return 0, false, fmt.Errorf("worker: default interval must be positive, got %s", defaultInterval.String())
	}
	if override == "" {
		return defaultInterval, false, nil
	}
	interval, err := time.ParseDuration(override)
	if err != nil {
		return 0, false, fmt.Errorf("worker: invalid --interval %q: %w", override, err)
	}
	if interval <= 0 {
		return 0, false, fmt.Errorf("worker: --interval must be positive, got %s", interval.String())
	}
	return interval, true, nil
}

type workerScanFunc func(context.Context) (worker.Result, domain.Token, error)

func runWorkerLoop(ctx context.Context, logger *slog.Logger, interval time.Duration, scan workerScanFunc) error {
	if interval <= 0 {
		return fmt.Errorf("worker: interval must be positive, got %s", interval.String())
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		result, actor, err := scan(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}
			logger.Error("worker tick failed",
				slog.String("token_id", actor.ID.String()),
				slog.String("token_source", string(actor.Source)),
				slog.String("error", err.Error()),
			)
		} else {
			logWorkerResult(logger, "worker tick complete", actor, result)
		}

		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func logWorkerResult(logger *slog.Logger, msg string, actor domain.Token, result worker.Result) {
	logger.Info(msg,
		slog.String("token_id", actor.ID.String()),
		slog.String("token_source", string(actor.Source)),
		slog.Int("network_commands_expired", result.NetworkCommandsExpired),
		slog.Int("scanned", result.Scanned),
		slog.Int("breaches_emitted", result.BreachesEmitted),
		slog.Int("breaches_already_recorded", result.BreachesAlreadyRecorded),
		slog.Int("patience_escalations_requested", result.PatienceEscalationsRequested),
		slog.Int("patience_escalations_already_requested", result.PatienceEscalationsAlreadyRequested),
		slog.Int("patience_escalations_skipped_awaiting_human", result.PatienceEscalationsSkippedAwaitingHuman),
		slog.Int("patience_dispatches_requested", result.PatienceDispatchesRequested),
		slog.Int("patience_dispatches_already_requested", result.PatienceDispatchesAlreadyRequested),
		slog.Int("scribe_candidates", result.ScribeCandidatesScanned),
		slog.Int("scribe_children_spawned", result.ScribeChildrenSpawned),
		slog.Int("scribe_children_already_present", result.ScribeChildrenAlreadyPresent),
		slog.Int("review_candidates", result.ReviewCandidatesScanned),
		slog.Int("review_children_spawned", result.ReviewChildrenSpawned),
		slog.Int("review_children_already_present", result.ReviewChildrenAlreadyPresent),
		slog.Int("review_skipped_missing_cultivar", result.ReviewPassSkippedMissingCultivar),
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
		slog.Int("convergence_malformed_payloads_skipped", result.ConvergenceMalformedPayloadsSkipped),
	)
}

func formatWorkerOnceResult(result worker.Result) string {
	return fmt.Sprintf("worker --once: network_commands_expired=%d scanned=%d emitted=%d already_recorded=%d patience_escalations=%d patience_escalations_already_requested=%d patience_escalations_skipped_awaiting_human=%d patience_dispatches=%d patience_dispatches_already_requested=%d scribe_candidates=%d scribe_children_spawned=%d scribe_children_already_present=%d review_candidates=%d review_children_spawned=%d review_children_already_present=%d review_skipped_missing_cultivar=%d dispatch_candidates=%d dispatch_requested=%d dispatch_already_requested=%d dispatch_skipped_missing_cultivar=%d convergence_candidates=%d convergence_verdicts=%d stale_inputs_skipped=%d accepts=%d retries=%d escalations=%d malformed_payloads_skipped=%d",
		result.NetworkCommandsExpired,
		result.Scanned,
		result.BreachesEmitted,
		result.BreachesAlreadyRecorded,
		result.PatienceEscalationsRequested,
		result.PatienceEscalationsAlreadyRequested,
		result.PatienceEscalationsSkippedAwaitingHuman,
		result.PatienceDispatchesRequested,
		result.PatienceDispatchesAlreadyRequested,
		result.ScribeCandidatesScanned,
		result.ScribeChildrenSpawned,
		result.ScribeChildrenAlreadyPresent,
		result.ReviewCandidatesScanned,
		result.ReviewChildrenSpawned,
		result.ReviewChildrenAlreadyPresent,
		result.ReviewPassSkippedMissingCultivar,
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
		result.ConvergenceMalformedPayloadsSkipped,
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
  MERISTEM_TOKEN=mrs_<system> meristem worker [--interval=DURATION] [--budget=DURATION]
  MERISTEM_TOKEN=mrs_<system> meristem worker --once [--budget=DURATION]
  MERISTEM_TOKEN=mrs_<system> meristem worker daemon [--interval=DURATION] [--budget=DURATION]

Runs the deterministic reconciler. The default form is an always-on daemon:
it runs one tick immediately, then repeats every --interval until SIGINT or
SIGTERM. Each tick runs checklist convergence for running work_items with
suggested_convergence_checks, then reads remaining non-terminal work_items,
compares dwell time to the per-state budget, appends one patience.breached
event per observed breach, and routes breached state epochs to dispatch or
human attention unless they are already waiting on human review. Re-running
with the same convergence signals does not consume a new convergence attempt.

  --interval=DURATION interval between daemon ticks. Default: active policy
                      profile's WorkerTickInterval (steady=30s, bring-up=60s).
  --budget=DURATION   override the per-state defaults with one uniform
                      budget applied to every non-terminal state. Intended
                      for operator/CI verification, not production policy.
                      Examples: --budget=1m, --budget=10s, --budget=72h.

Use --once for a single manual or CI tick. The daemon and --once paths both
require MERISTEM_TOKEN to authenticate as a dedicated source=system token.
`)
}
