package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/worker"
)

func TestFormatWorkerOnceResultIncludesDispatchCounters(t *testing.T) {
	line := formatWorkerOnceResult(worker.Result{
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
		ConvergenceCandidatesScanned:            19,
		ConvergenceVerdictsRecorded:             20,
		ConvergenceVerdictsAlreadyRecorded:      21,
		ConvergenceStaleInputsSkipped:           22,
		ConvergenceAccepts:                      23,
		ConvergenceRetries:                      24,
		ConvergenceEscalations:                  25,
	})

	for _, want := range []string{
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

func TestRunWorkerLoopRejectsNonPositiveInterval(t *testing.T) {
	err := runWorkerLoop(context.Background(), discardLogger(), 0, func(context.Context) (worker.Result, domain.Token, error) {
		return worker.Result{}, systemTestToken(), nil
	})
	if err == nil || !strings.Contains(err.Error(), "interval must be positive") {
		t.Fatalf("runWorkerLoop error = %v, want interval validation", err)
	}
}

func TestWorkerUsageMentionsDaemonAndOnce(t *testing.T) {
	var b strings.Builder
	workerUsage(&b)
	out := b.String()
	for _, want := range []string{
		"meristem worker [--interval=DURATION]",
		"meristem worker --once",
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
