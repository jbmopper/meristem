package listeneractivation

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/projections"
)

func RegisterProjectors(registry *projections.Registry) {
	registry.Register(requestedProjector{})
	for _, kind := range []string{
		domain.EventListenerActivationDispatching,
		domain.EventListenerActivationAccepted,
		domain.EventListenerActivationCompleted,
		domain.EventListenerActivationFailed,
		domain.EventListenerActivationAmbiguous,
	} {
		registry.Register(outcomeProjector{kind: kind})
	}
}

type requestedProjector struct{}

func (requestedProjector) Kind() string { return domain.EventListenerActivationRequested }

func (requestedProjector) Apply(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	var p struct {
		PayloadVersion    int    `json:"payload_version"`
		ListenerID        string `json:"listener_id"`
		WorkItemID        string `json:"work_item_id"`
		AssignmentEventID string `json:"assignment_event_id"`
		DemandEventID     string `json:"demand_event_id"`
		Attempt           int    `json:"attempt"`
		AdapterKind       string `json:"adapter_kind"`
		BindingGeneration string `json:"binding_generation"`
	}
	if err := decode(event.Payload, &p); err != nil {
		return err
	}
	if p.PayloadVersion != PayloadVersion || p.Attempt < 1 || p.AdapterKind == "" || p.BindingGeneration == "" {
		return fmt.Errorf("listener.activation_requested: invalid payload")
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO listener_activations (
			id, listener_id, work_item_id, assignment_event_id, demand_event_id,
			attempt, adapter_kind, binding_generation, state,
			last_outcome_event_id, state_event_id, state_event_seq,
			created_at, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$10,$11,$12,$12)
		ON CONFLICT (id) DO NOTHING
	`, event.SubjectID, p.ListenerID, p.WorkItemID, p.AssignmentEventID,
		p.DemandEventID, p.Attempt, p.AdapterKind, p.BindingGeneration,
		StateRequested, event.ID, event.Seq, event.OccurredAt)
	return err
}

type outcomeProjector struct{ kind string }

func (p outcomeProjector) Kind() string { return p.kind }

func (p outcomeProjector) Apply(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	var payload struct {
		PayloadVersion     int          `json:"payload_version"`
		From               State        `json:"from"`
		Mode               DispatchMode `json:"mode"`
		ConsumerGeneration string       `json:"consumer_generation"`
		LeaseExpiresAt     *time.Time   `json:"lease_expires_at"`
		NextRetryAt        *time.Time   `json:"next_retry_at"`
		Reason             string       `json:"reason"`
	}
	if err := decode(event.Payload, &payload); err != nil {
		return err
	}
	if payload.PayloadVersion != PayloadVersion {
		return fmt.Errorf("%s: unknown payload version %d", event.Kind, payload.PayloadVersion)
	}
	var current State
	var seq int64
	if err := tx.QueryRow(ctx, `SELECT state,state_event_seq FROM listener_activations WHERE id=$1 FOR UPDATE`, event.SubjectID).Scan(&current, &seq); err != nil {
		return err
	}
	if seq >= event.Seq {
		return nil
	}
	if payload.From != current {
		return fmt.Errorf("%s: payload from %q disagrees with history state %q", event.Kind, payload.From, current)
	}
	next, err := stateForKind(event.Kind)
	if err != nil {
		return err
	}
	if !validActivationTransition(current, next) {
		return fmt.Errorf("%s: invalid activation transition %q -> %q", event.Kind, current, next)
	}
	if next == StateDispatching {
		if payload.Mode != ModeDispatch && payload.Mode != ModeReconcile {
			return fmt.Errorf("%s: invalid dispatch mode", event.Kind)
		}
		if (current == StateAmbiguous) != (payload.Mode == ModeReconcile) {
			return fmt.Errorf("%s: mode %q disagrees with source state %q", event.Kind, payload.Mode, current)
		}
		if payload.ConsumerGeneration == "" || payload.LeaseExpiresAt == nil {
			return fmt.Errorf("%s: consumer generation and lease are required", event.Kind)
		}
		_, err = tx.Exec(ctx, `
			UPDATE listener_activations SET
			  state=$2, dispatch_mode=$3, consumer_generation=$4,
			  lease_expires_at=$5, next_retry_at=NULL,
			  dispatch_count=dispatch_count+CASE WHEN $3='dispatch' THEN 1 ELSE 0 END,
			  reconcile_count=reconcile_count+CASE WHEN $3='reconcile' THEN 1 ELSE 0 END,
			  last_reason='', last_outcome_event_id=$6,
			  state_event_id=$6, state_event_seq=$7, updated_at=$8
			WHERE id=$1
		`, event.SubjectID, next, payload.Mode, payload.ConsumerGeneration,
			payload.LeaseExpiresAt, event.ID, event.Seq, event.OccurredAt)
		return err
	}
	if next == StateAccepted {
		if payload.LeaseExpiresAt == nil {
			return fmt.Errorf("%s: accepted lease is required", event.Kind)
		}
		_, err = tx.Exec(ctx, `
			UPDATE listener_activations SET state=$2, lease_expires_at=$3,
			  last_reason=$4, last_outcome_event_id=$5,
			  state_event_id=$5, state_event_seq=$6, updated_at=$7
			WHERE id=$1
		`, event.SubjectID, next, payload.LeaseExpiresAt, payload.Reason,
			event.ID, event.Seq, event.OccurredAt)
		return err
	}
	_, err = tx.Exec(ctx, `
		UPDATE listener_activations SET state=$2, dispatch_mode=NULL,
		  consumer_generation=NULL, lease_expires_at=NULL, next_retry_at=$3,
		  last_reason=$4, last_outcome_event_id=$5,
		  state_event_id=$5, state_event_seq=$6, updated_at=$7
		WHERE id=$1
	`, event.SubjectID, next, payload.NextRetryAt, payload.Reason,
		event.ID, event.Seq, event.OccurredAt)
	return err
}

func validActivationTransition(from, to State) bool {
	switch from {
	case StateRequested, StateFailed:
		return to == StateDispatching
	case StateAmbiguous:
		return to == StateDispatching
	case StateDispatching:
		return to == StateAccepted || to == StateCompleted || to == StateFailed || to == StateAmbiguous
	case StateAccepted:
		return to == StateCompleted || to == StateFailed || to == StateAmbiguous
	default:
		return false
	}
}

func stateForKind(kind string) (State, error) {
	switch kind {
	case domain.EventListenerActivationDispatching:
		return StateDispatching, nil
	case domain.EventListenerActivationAccepted:
		return StateAccepted, nil
	case domain.EventListenerActivationCompleted:
		return StateCompleted, nil
	case domain.EventListenerActivationFailed:
		return StateFailed, nil
	case domain.EventListenerActivationAmbiguous:
		return StateAmbiguous, nil
	default:
		return "", fmt.Errorf("listeneractivation: unknown outcome kind %q", kind)
	}
}

func decode(payload any, out any) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, out)
}
