package main

import (
	"strings"
	"testing"

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
		ScribeCandidatesScanned:                 6,
		ScribeChildrenSpawned:                   7,
		ScribeChildrenAlreadyPresent:            8,
		DispatchCandidatesScanned:               9,
		DispatchesRequested:                     10,
		DispatchesAlreadyRequested:              11,
		DispatchesSkippedMissingCultivar:        12,
		ConvergenceCandidatesScanned:            13,
		ConvergenceVerdictsRecorded:             14,
		ConvergenceVerdictsAlreadyRecorded:      15,
		ConvergenceStaleInputsSkipped:           16,
		ConvergenceAccepts:                      17,
		ConvergenceRetries:                      18,
		ConvergenceEscalations:                  19,
	})

	for _, want := range []string{
		"dispatch_candidates=9",
		"dispatch_requested=10",
		"dispatch_already_requested=11",
		"dispatch_skipped_missing_cultivar=12",
		"convergence_verdicts=29",
	} {
		if !strings.Contains(line, want) {
			t.Fatalf("formatted worker result missing %q:\n%s", want, line)
		}
	}
}
