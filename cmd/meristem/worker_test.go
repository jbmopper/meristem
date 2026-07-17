package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/buildguard"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/worker"
)

func TestFormatWorkerOnceResultIncludesDispatchCounters(t *testing.T) {
	line := formatWorkerOnceResult(worker.Result{
		NetworkCommandsExpired:                  26,
		Scanned:                                 10,
		BreachesEmitted:                         1,
		BreachesAlreadyRecorded:                 2,
		PatienceEscalationsRequested:            3,
		PatienceEscalationsAlreadyRequested:     4,
		PatienceEscalationsSkippedAwaitingHuman: 5,
		PatienceDispatchesRequested:             6,
		PatienceDispatchesAlreadyRequested:      7,
		ScribeCandidatesScanned:                 8,
		ScribeChildrenSpawned:                   9,
		ScribeChildrenAlreadyPresent:            10,
		ReviewCandidatesScanned:                 11,
		ReviewChildrenSpawned:                   12,
		ReviewChildrenAlreadyPresent:            13,
		ReviewPassSkippedMissingCultivar:        14,
		DispatchCandidatesScanned:               15,
		DispatchesRequested:                     16,
		DispatchesAlreadyRequested:              17,
		DispatchesSkippedMissingCultivar:        18,
		DispatchJobsReconciledCanceled:          27,
		ReviewDispatchJobsClaimed:               28,
		ReviewDispatchJobsStarted:               29,
		ReviewDispatchJobsAlreadyDone:           30,
		ReviewDispatchJobsCanceled:              31,
		ReviewDispatchJobsDormant:               32,
		ConvergenceCandidatesScanned:            19,
		ConvergenceVerdictsRecorded:             20,
		ConvergenceVerdictsAlreadyRecorded:      21,
		ConvergenceStaleInputsSkipped:           22,
		ConvergenceAccepts:                      23,
		ConvergenceRetries:                      24,
		ConvergenceEscalations:                  25,
	})

	for _, want := range []string{
		"network_commands_expired=26",
		"patience_dispatches=6",
		"patience_dispatches_already_requested=7",
		"review_candidates=11",
		"review_children_spawned=12",
		"review_children_already_present=13",
		"review_skipped_missing_cultivar=14",
		"dispatch_candidates=15",
		"dispatch_requested=16",
		"dispatch_already_requested=17",
		"dispatch_skipped_missing_cultivar=18",
		"dispatch_jobs_reconciled_canceled=27",
		"review_dispatch_jobs_claimed=28",
		"review_dispatch_jobs_started=29",
		"review_dispatch_jobs_already_done=30",
		"review_dispatch_jobs_canceled=31",
		"review_dispatch_jobs_dormant=32",
		"convergence_verdicts=41",
	} {
		if !strings.Contains(line, want) {
			t.Fatalf("formatted worker result missing %q:\n%s", want, line)
		}
	}
}

func TestRunWorkerLoopScansImmediatelyAndStopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var calls atomic.Int32
	err := runWorkerLoop(ctx, discardLogger(), time.Millisecond, func(context.Context) (worker.Result, domain.Token, error) {
		n := calls.Add(1)
		if n == 2 {
			cancel()
		}
		return worker.Result{Scanned: int(n)}, systemTestToken(), nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("runWorkerLoop error = %v, want context.Canceled", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("scan calls = %d, want 2", got)
	}
}

func TestRunWorkerLoopContinuesAfterScanError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var calls atomic.Int32
	err := runWorkerLoop(ctx, discardLogger(), time.Millisecond, func(context.Context) (worker.Result, domain.Token, error) {
		n := calls.Add(1)
		if n == 1 {
			return worker.Result{}, systemTestToken(), errors.New("temporary scan failure")
		}
		cancel()
		return worker.Result{Scanned: int(n)}, systemTestToken(), nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("runWorkerLoop error = %v, want context.Canceled", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("scan calls = %d, want 2", got)
	}
}

func TestRunWorkerLoopExitsOnBuildGuardBlock(t *testing.T) {
	var calls atomic.Int32
	err := runWorkerLoop(context.Background(), discardLogger(), time.Hour, func(context.Context) (worker.Result, domain.Token, error) {
		calls.Add(1)
		return worker.Result{}, systemTestToken(), fmt.Errorf("worker scan: %w", buildguard.ErrBlocked)
	})
	if !errors.Is(err, buildguard.ErrBlocked) {
		t.Fatalf("runWorkerLoop error = %v, want buildguard.ErrBlocked", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("scan calls = %d, want 1", got)
	}
}

func TestRunWorkerLoopRejectsNonPositiveInterval(t *testing.T) {
	err := runWorkerLoop(context.Background(), discardLogger(), 0, func(context.Context) (worker.Result, domain.Token, error) {
		return worker.Result{}, systemTestToken(), nil
	})
	if err == nil || !strings.Contains(err.Error(), "interval must be positive") {
		t.Fatalf("runWorkerLoop error = %v, want interval validation", err)
	}
}

func TestResolveWorkerInterval(t *testing.T) {
	t.Run("uses profile default when unset", func(t *testing.T) {
		got, overridden, err := resolveWorkerInterval(45*time.Second, "")
		if err != nil {
			t.Fatalf("resolveWorkerInterval: %v", err)
		}
		if got != 45*time.Second || overridden {
			t.Fatalf("got (%s, %v), want (45s, false)", got, overridden)
		}
	})

	t.Run("accepts explicit override", func(t *testing.T) {
		got, overridden, err := resolveWorkerInterval(45*time.Second, "90s")
		if err != nil {
			t.Fatalf("resolveWorkerInterval: %v", err)
		}
		if got != 90*time.Second || !overridden {
			t.Fatalf("got (%s, %v), want (90s, true)", got, overridden)
		}
	})

	t.Run("rejects invalid override", func(t *testing.T) {
		if _, _, err := resolveWorkerInterval(45*time.Second, "nope"); err == nil {
			t.Fatal("expected invalid override to fail")
		}
	})

	t.Run("rejects non-positive override", func(t *testing.T) {
		if _, _, err := resolveWorkerInterval(45*time.Second, "0s"); err == nil {
			t.Fatal("expected non-positive override to fail")
		}
	})
}

func TestWorkerUsageMentionsDaemonAndOnce(t *testing.T) {
	var b strings.Builder
	workerUsage(&b)
	out := b.String()
	for _, want := range []string{
		"meristem worker [--interval=DURATION]",
		"meristem worker --once",
		"steady=30s, bring-up=60s",
		"always-on daemon",
		"SIGINT",
		"SIGTERM",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("usage missing %q:\n%s", want, out)
		}
	}
}

func TestLogWorkerResultIncludesSystemTokenFields(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	actor := systemTestToken()

	logWorkerResult(logger, "worker tick complete", actor, worker.Result{Scanned: 3})

	out := buf.String()
	for _, want := range []string{
		`"msg":"worker tick complete"`,
		`"token_id":"` + actor.ID.String() + `"`,
		`"token_source":"system"`,
		`"scanned":3`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("log output missing %q:\n%s", want, out)
		}
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func systemTestToken() domain.Token {
	return domain.Token{
		ID:     uuid.NewSHA1(uuid.NameSpaceURL, []byte("meristem-test-worker-token")),
		Source: domain.SourceSystem,
	}
}
