package worker

import (
	"context"
	"fmt"
	"time"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/jobqueue"
	"github.com/jbmopper/meristem/internal/workitems"
)

const (
	reviewDispatchLease     = 30 * time.Second
	reviewDispatchBatchSize = 8
)

type dispatchExecutionResult struct {
	DispatchJobsReconciledCanceled int
	ReviewDispatchJobsClaimed      int
	ReviewDispatchJobsStarted      int
	ReviewDispatchJobsAlreadyDone  int
	ReviewDispatchJobsCanceled     int
	ReviewDispatchJobsDormant      int
}

// executeReviewDispatches is intentionally narrower than a generic queue
// executor. The deterministic worker may admit reviewer children into running;
// it must not claim ordinary agent dispatches and thereby imply that their
// external work was performed. Claims use the queue's bounded lease and batch
// size, while StartReviewDispatch performs the final event-backed revalidation
// and atomic job completion.
func (w *Worker) executeReviewDispatches(ctx context.Context) (dispatchExecutionResult, error) {
	queue := jobqueue.NewService(w.pool)
	result := dispatchExecutionResult{}

	canceled, err := queue.ReconcileDispatchJobs(ctx)
	if err != nil {
		return result, err
	}
	result.DispatchJobsReconciledCanceled = canceled

	actor := domain.Token{ID: *w.actor, Source: domain.SourceSystem}
	service := workitems.NewService(w.pool, w.writer)
	for range reviewDispatchBatchSize {
		job, found, err := queue.ClaimNextReview(ctx, reviewDispatchLease)
		if err != nil {
			return result, err
		}
		if !found {
			break
		}
		result.ReviewDispatchJobsClaimed++

		started, err := service.StartReviewDispatch(ctx, job.ID, actor)
		if err != nil {
			return result, fmt.Errorf("start review dispatch job %s: %w", job.ID, err)
		}
		switch started.Outcome {
		case workitems.ReviewDispatchStarted:
			result.ReviewDispatchJobsStarted++
		case workitems.ReviewDispatchAlreadyDone:
			result.ReviewDispatchJobsAlreadyDone++
		case workitems.ReviewDispatchCanceled:
			result.ReviewDispatchJobsCanceled++
		case workitems.ReviewDispatchDormant:
			result.ReviewDispatchJobsDormant++
		default:
			return result, fmt.Errorf("review dispatch job %s returned unknown outcome %q", job.ID, started.Outcome)
		}
	}
	return result, nil
}
