//go:build !convergence_worker_experiment

package worker

import "context"

// convergencePassResult tracks the effects of one ScanOnce convergence kernel.
// The default build keeps lifecycle-driving convergence disabled until verdict
// persistence and pattern declaration land. Build with
// -tags=convergence_worker_experiment to compile the experimental driver.
type convergencePassResult struct {
	ConvergenceCandidatesScanned       int
	ConvergenceVerdictsRecorded        int
	ConvergenceVerdictsAlreadyRecorded int
	ConvergenceAccepts                 int
	ConvergenceRetries                 int
	ConvergenceEscalations             int
}

func (w *Worker) scanConvergence(ctx context.Context) (convergencePassResult, error) {
	return convergencePassResult{}, nil
}
