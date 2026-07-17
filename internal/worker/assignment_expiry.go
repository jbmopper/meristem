package worker

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/workitems"
)

const assignmentExpiryBatchSize = 100

// expireAssignments scans a bounded batch from the durable projection, then
// revalidates each candidate through workitems.ExpireAssignment. The service
// owns the global work_item -> assignment row lock order; multiple workers may
// race safely and only one exact release event can commit.
func (w *Worker) expireAssignments(ctx context.Context) (int, error) {
	rows, err := w.pool.Query(ctx, `
		SELECT work_item_id
		FROM work_item_assignment_state
		WHERE holder_token_id IS NOT NULL
		  AND expires_at <= transaction_timestamp()
		ORDER BY expires_at, work_item_id
		LIMIT $1
	`, assignmentExpiryBatchSize)
	if err != nil {
		return 0, fmt.Errorf("worker: query due assignments: %w", err)
	}
	var candidates []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, fmt.Errorf("worker: scan due assignment: %w", err)
		}
		candidates = append(candidates, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, fmt.Errorf("worker: iterate due assignments: %w", err)
	}
	rows.Close()

	actor := domain.Token{ID: *w.actor, Source: domain.SourceSystem}
	expired := 0
	service := workitems.NewService(w.pool, w.writer)
	for _, id := range candidates {
		fresh, err := service.ExpireAssignment(ctx, id, actor)
		if err != nil {
			return expired, fmt.Errorf("worker: expire assignment for %s: %w", id, err)
		}
		if fresh {
			expired++
		}
	}
	return expired, nil
}
