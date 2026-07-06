package worker

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/domain"
)

const (
	defaultPatienceAttentionLimit = 100
	maxPatienceAttentionLimit     = 500
)

type PatienceAttentionStatus string

const (
	PatienceAttentionOpen     PatienceAttentionStatus = "open"
	PatienceAttentionResolved PatienceAttentionStatus = "resolved"
)

// PatienceAttention is the replay-safe read model for patience.breached.
//
// A breach stays open only while the current work_items projection still names
// the same state epoch recorded in the breach payload. Any later transition
// changes either state or state_entered_at, which deterministically resolves the
// breach without appending a second event.
type PatienceAttention struct {
	EventID               uuid.UUID
	WorkItemID            uuid.UUID
	BreachedAt            time.Time
	State                 domain.WorkItemState
	BudgetSeconds         int64
	BudgetSource          string
	EscalationRule        domain.EscalationRule
	StateEnteredAtUnix    int64
	Cultivar              string
	CurrentState          domain.WorkItemState
	CurrentStateEnteredAt time.Time
	Status                PatienceAttentionStatus
}

type PatienceAttentionOptions struct {
	Limit           int
	IncludeResolved bool
}

type patienceAttentionPayload struct {
	State              string `json:"state"`
	BudgetSeconds      int64  `json:"budget_seconds"`
	BudgetSource       string `json:"budget_source"`
	EscalationRule     string `json:"escalation_rule"`
	StateEnteredAtUnix int64  `json:"state_entered_at_unix"`
	Cultivar           string `json:"cultivar"`
}

// ListPatienceAttention folds patience.breached rows against the current
// work_items projection. The projection is itself replay-derived, so after a
// rebuild the same event log produces the same open/resolved verdicts.
func ListPatienceAttention(ctx context.Context, pool *pgxpool.Pool, opts PatienceAttentionOptions) ([]PatienceAttention, error) {
	if pool == nil {
		return nil, errors.New("worker: pool is required")
	}
	limit := opts.Limit
	if limit <= 0 {
		limit = defaultPatienceAttentionLimit
	}
	if limit > maxPatienceAttentionLimit {
		limit = maxPatienceAttentionLimit
	}
	rows, err := pool.Query(ctx, `
		SELECT e.id, e.occurred_at, e.subject_id, e.payload, wi.state, wi.state_entered_at
		FROM events e
		LEFT JOIN work_items wi ON wi.id = e.subject_id
		WHERE e.subject_kind = $1
		  AND e.kind = $2
		  AND (
		    $3::boolean
		    OR (
		      wi.id IS NOT NULL
		      AND wi.state = e.payload->>'state'
		      AND floor(extract(epoch FROM wi.state_entered_at))::bigint = (e.payload->>'state_entered_at_unix')::bigint
		    )
		  )
		ORDER BY e.seq DESC
		LIMIT $4
	`, domain.SubjectWorkItem, domain.EventPatienceBreached, opts.IncludeResolved, limit)
	if err != nil {
		return nil, fmt.Errorf("worker: list patience attention: %w", err)
	}
	defer rows.Close()

	var out []PatienceAttention
	for rows.Next() {
		var (
			item           PatienceAttention
			rawPayload     []byte
			currentState   sql.NullString
			currentEntered sql.NullTime
		)
		if err := rows.Scan(&item.EventID, &item.BreachedAt, &item.WorkItemID, &rawPayload, &currentState, &currentEntered); err != nil {
			return nil, fmt.Errorf("worker: scan patience attention: %w", err)
		}
		var payload patienceAttentionPayload
		if err := json.Unmarshal(rawPayload, &payload); err != nil {
			return nil, fmt.Errorf("worker: decode patience attention payload for %s: %w", item.EventID, err)
		}
		item.State = domain.WorkItemState(payload.State)
		item.BudgetSeconds = payload.BudgetSeconds
		item.BudgetSource = payload.BudgetSource
		item.EscalationRule = domain.EscalationRule(payload.EscalationRule)
		item.StateEnteredAtUnix = payload.StateEnteredAtUnix
		item.Cultivar = payload.Cultivar
		item.Status = PatienceAttentionResolved
		if currentState.Valid {
			item.CurrentState = domain.WorkItemState(currentState.String)
		}
		if currentEntered.Valid {
			item.CurrentStateEnteredAt = currentEntered.Time.UTC()
		}
		if currentState.Valid &&
			currentEntered.Valid &&
			currentState.String == payload.State &&
			currentEntered.Time.UTC().Unix() == payload.StateEnteredAtUnix {
			item.Status = PatienceAttentionOpen
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("worker: iterate patience attention: %w", err)
	}
	return out, nil
}
