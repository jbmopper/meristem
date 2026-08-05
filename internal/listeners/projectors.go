package listeners

// Synchronous projectors for the listener_registrations projection. Replay
// is byte-for-byte deterministic: every field written derives from the event
// row alone (payload, id, seq, occurred_at), never from wall clocks or prior
// process state.

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/projections"
)

func RegisterProjectors(registry *projections.Registry) {
	registry.Register(registeredProjector{})
	registry.Register(credentialBoundProjector{})
	registry.Register(policySetProjector{})
	registry.Register(retiredProjector{})
}

type registeredProjector struct{}

func (registeredProjector) Kind() string { return domain.EventListenerRegistered }

func (registeredProjector) Apply(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	var payload struct {
		Name                     string    `json:"name"`
		PrincipalTokenID         uuid.UUID `json:"principal_token_id"`
		Provider                 string    `json:"provider"`
		Capabilities             []string  `json:"capabilities"`
		MaxConcurrentAssignments int       `json:"max_concurrent_assignments"`
	}
	if err := decodePayload(event, &payload); err != nil {
		return err
	}
	capabilities, err := json.Marshal(payload.Capabilities)
	if err != nil {
		return fmt.Errorf("listeners: encode capabilities: %w", err)
	}
	if payload.MaxConcurrentAssignments == 0 {
		payload.MaxConcurrentAssignments = 1
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO listener_registrations (
			id, name, principal_token_id, provider, capabilities,
			max_concurrent_assignments, state_event_id, state_event_seq,
			created_at, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$9)
		ON CONFLICT (id) DO NOTHING`,
		event.SubjectID, payload.Name, payload.PrincipalTokenID, payload.Provider,
		capabilities, payload.MaxConcurrentAssignments, event.ID, event.Seq, event.OccurredAt,
	)
	return err
}

type credentialBoundProjector struct{}

func (credentialBoundProjector) Kind() string { return domain.EventListenerCredentialBound }

func (credentialBoundProjector) Apply(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	var payload struct {
		PrincipalTokenID uuid.UUID `json:"principal_token_id"`
	}
	if err := decodePayload(event, &payload); err != nil {
		return err
	}
	// Stale-guard on seq keeps replay and concurrent execution idempotent:
	// an older binding event can never overwrite a newer projection row.
	_, err := tx.Exec(ctx, `
		UPDATE listener_registrations
		SET principal_token_id=$2, state_event_id=$3, state_event_seq=$4, updated_at=$5
		WHERE id=$1 AND state_event_seq < $4`,
		event.SubjectID, payload.PrincipalTokenID, event.ID, event.Seq, event.OccurredAt,
	)
	return err
}

type policySetProjector struct{}

func (policySetProjector) Kind() string { return domain.EventListenerPolicySet }

func (policySetProjector) Apply(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	var payload struct {
		PayloadVersion           int             `json:"payload_version"`
		ListenerID               uuid.UUID       `json:"listener_id"`
		Projection               string          `json:"projection"`
		Predicates               json.RawMessage `json:"predicates"`
		Capabilities             []string        `json:"capabilities"`
		MaxConcurrentAssignments int             `json:"max_concurrent_assignments"`
		Focus                    string          `json:"focus"`
		PredicateFingerprint     string          `json:"predicate_fingerprint"`
	}
	if err := decodePayload(event, &payload); err != nil {
		return err
	}
	policy := Policy{
		PayloadVersion:           payload.PayloadVersion,
		ListenerID:               event.SubjectID,
		Projection:               payload.Projection,
		Capabilities:             payload.Capabilities,
		MaxConcurrentAssignments: payload.MaxConcurrentAssignments,
		Focus:                    payload.Focus,
	}
	if len(payload.Predicates) > 0 {
		if err := json.Unmarshal(payload.Predicates, &policy.Predicates); err != nil {
			return fmt.Errorf("listeners: decode policy predicates: %w", err)
		}
	}
	blob, err := json.Marshal(policy)
	if err != nil {
		return fmt.Errorf("listeners: encode policy: %w", err)
	}
	_, err = tx.Exec(ctx, `
		UPDATE listener_registrations
		SET policy=$2, policy_fingerprint=$3, policy_event_id=$4,
		    state_event_id=$4, state_event_seq=$5, updated_at=$6
		WHERE id=$1 AND state_event_seq < $5`,
		event.SubjectID, blob, payload.PredicateFingerprint, event.ID, event.Seq, event.OccurredAt,
	)
	return err
}

type retiredProjector struct{}

func (retiredProjector) Kind() string { return domain.EventListenerRetired }

func (retiredProjector) Apply(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	_, err := tx.Exec(ctx, `
		UPDATE listener_registrations
		SET retired_at=$2, state_event_id=$3, state_event_seq=$4, updated_at=$2
		WHERE id=$1 AND state_event_seq < $4`,
		event.SubjectID, event.OccurredAt, event.ID, event.Seq,
	)
	return err
}

func decodePayload(event domain.Event, out any) error {
	raw, err := json.Marshal(event.Payload)
	if err != nil {
		return fmt.Errorf("listeners: re-encode %s payload: %w", event.Kind, err)
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("listeners: decode %s payload: %w", event.Kind, err)
	}
	return nil
}
