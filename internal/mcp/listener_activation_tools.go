package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/listeneractivation"
)

func (s *Server) toolListenersEnsureActivation() Tool {
	return Tool{
		Name: "listeners.ensure_activation", Mutates: true,
		Description: "Idempotently create the durable adapter activation for one exact listener-bound assignment generation.",
		InputSchema: schemaObject([]string{"id", "assignment_event_id", "binding_generation"}, map[string]any{
			"id":                      schemaString("Listener uuid."),
			"assignment_event_id":     schemaString("Exact work_item.assigned event uuid."),
			"binding_generation":      schemaString("Opaque adapter-local binding generation; never a task id or bearer."),
			"task_principal_token_id": schemaString("Optional separate task credential UUID folded into binding_generation."),
			"attempt":                 map[string]any{"type": "integer", "description": "Adapter attempt number; defaults to 1."},
		}),
		Handler: func(ctx context.Context, actor domain.Token, raw json.RawMessage) (any, error) {
			if s.deps.ListenerActivations == nil {
				return nil, errors.New("listener activation service not configured")
			}
			var args struct {
				ID                string `json:"id"`
				AssignmentEventID string `json:"assignment_event_id"`
				BindingGeneration string `json:"binding_generation"`
				TaskPrincipalID   string `json:"task_principal_token_id"`
				Attempt           int    `json:"attempt"`
			}
			if err := decodeArgs(raw, &args); err != nil {
				return nil, err
			}
			listenerID, err := parseUUID(args.ID, "id")
			if err != nil {
				return nil, err
			}
			assignmentID, err := parseUUID(args.AssignmentEventID, "assignment_event_id")
			if err != nil {
				return nil, err
			}
			taskPrincipalID := uuid.Nil
			if args.TaskPrincipalID != "" {
				taskPrincipalID, err = parseUUID(args.TaskPrincipalID, "task_principal_token_id")
				if err != nil || taskPrincipalID == uuid.Nil || args.TaskPrincipalID != taskPrincipalID.String() {
					return nil, fmt.Errorf("task_principal_token_id must be one canonical non-nil uuid")
				}
			}
			a, err := s.deps.ListenerActivations.Ensure(ctx, listeneractivation.EnsureInput{
				ListenerID: listenerID, AssignmentEventID: assignmentID,
				BindingGeneration: args.BindingGeneration, TaskPrincipalID: taskPrincipalID,
				Attempt: args.Attempt, Actor: actor,
			})
			if err != nil {
				return nil, listenerActivationToolErr(err)
			}
			return map[string]any{"activation": toListenerActivationDTO(a)}, nil
		},
	}
}

func (s *Server) toolListenerActivationsBegin() Tool {
	return Tool{
		Name: "listener_activations.begin", Mutates: true,
		Description: "Claim one finite activation consumer generation and return dispatch, reconcile, wait, or terminal. Expired uncertain dispatches are forced through reconcile.",
		InputSchema: schemaObject([]string{"id", "consumer_generation"}, map[string]any{
			"id":                  schemaString("Activation uuid."),
			"consumer_generation": schemaString("Stable generation for the single active adapter consumer."),
		}),
		Handler: func(ctx context.Context, actor domain.Token, raw json.RawMessage) (any, error) {
			if s.deps.ListenerActivations == nil {
				return nil, errors.New("listener activation service not configured")
			}
			var args struct {
				ID                 string `json:"id"`
				ConsumerGeneration string `json:"consumer_generation"`
			}
			if err := decodeArgs(raw, &args); err != nil {
				return nil, err
			}
			id, err := parseUUID(args.ID, "id")
			if err != nil {
				return nil, err
			}
			result, err := s.deps.ListenerActivations.Begin(ctx, listeneractivation.BeginInput{
				ActivationID: id, ConsumerGeneration: args.ConsumerGeneration, Actor: actor,
			})
			if err != nil {
				return nil, listenerActivationToolErr(err)
			}
			return map[string]any{"action": result.Action, "activation": toListenerActivationDTO(result.Activation)}, nil
		},
	}
}

func (s *Server) toolListenerActivationsRecordReceipt() Tool {
	return Tool{
		Name: "listener_activations.record_receipt", Mutates: true,
		Description: "Record an adapter receipt against the exact activation state and consumer generation.",
		InputSchema: schemaObject([]string{"id", "observed_state_event_id", "consumer_generation", "outcome"}, map[string]any{
			"id":                      schemaString("Activation uuid."),
			"observed_state_event_id": schemaString("Exact activation state event observed before adapter contact."),
			"consumer_generation":     schemaString("Consumer generation holding the dispatch lease."),
			"outcome":                 schemaString("accepted, completed, failed, or ambiguous."),
			"reason":                  schemaString("Structural reason without event or work-item body content."),
		}),
		Handler: func(ctx context.Context, actor domain.Token, raw json.RawMessage) (any, error) {
			if s.deps.ListenerActivations == nil {
				return nil, errors.New("listener activation service not configured")
			}
			var args struct {
				ID                   string `json:"id"`
				ObservedStateEventID string `json:"observed_state_event_id"`
				ConsumerGeneration   string `json:"consumer_generation"`
				Outcome              string `json:"outcome"`
				Reason               string `json:"reason"`
			}
			if err := decodeArgs(raw, &args); err != nil {
				return nil, err
			}
			id, err := parseUUID(args.ID, "id")
			if err != nil {
				return nil, err
			}
			observed, err := parseUUID(args.ObservedStateEventID, "observed_state_event_id")
			if err != nil {
				return nil, err
			}
			a, err := s.deps.ListenerActivations.RecordReceipt(ctx, listeneractivation.ReceiptInput{
				ActivationID: id, ObservedStateEventID: observed,
				ConsumerGeneration: args.ConsumerGeneration,
				Outcome:            listeneractivation.State(args.Outcome), Reason: args.Reason, Actor: actor,
			})
			if err != nil {
				return nil, listenerActivationToolErr(err)
			}
			return map[string]any{"activation": toListenerActivationDTO(a)}, nil
		},
	}
}

func toListenerActivationDTO(a listeneractivation.Activation) map[string]any {
	out := map[string]any{
		"id": a.ID.String(), "listener_id": a.ListenerID.String(),
		"work_item_id": a.WorkItemID.String(), "assignment_event_id": a.AssignmentEventID.String(),
		"demand_event_id": a.DemandEventID.String(), "attempt": a.Attempt,
		"adapter_kind": a.AdapterKind, "binding_generation": a.BindingGeneration,
		"state": a.State, "dispatch_count": a.DispatchCount,
		"reconcile_count": a.ReconcileCount, "last_reason": a.LastReason,
		"last_outcome_event_id": a.LastOutcomeEventID.String(),
		"state_event_id":        a.StateEventID.String(),
		"created_at":            a.CreatedAt.UTC().Format(time.RFC3339Nano),
		"updated_at":            a.UpdatedAt.UTC().Format(time.RFC3339Nano),
	}
	if a.DispatchMode != "" {
		out["dispatch_mode"] = a.DispatchMode
	}
	if a.ConsumerGeneration != "" {
		out["consumer_generation"] = a.ConsumerGeneration
	}
	if a.LeaseExpiresAt != nil {
		out["lease_expires_at"] = a.LeaseExpiresAt.UTC().Format(time.RFC3339Nano)
	}
	if a.NextRetryAt != nil {
		out["next_retry_at"] = a.NextRetryAt.UTC().Format(time.RFC3339Nano)
	}
	return out
}

func listenerActivationToolErr(err error) error {
	switch {
	case errors.Is(err, listeneractivation.ErrNotFound):
		return replayableToolErr(notFoundToolError{msg: "listener_activation_not_found: " + err.Error()})
	case errors.Is(err, listeneractivation.ErrInvalidRequest),
		errors.Is(err, listeneractivation.ErrNotAuthorized),
		errors.Is(err, listeneractivation.ErrStaleState),
		errors.Is(err, listeneractivation.ErrNoActiveAssignment):
		return replayableToolErr(pureToolErr(err))
	default:
		return fmt.Errorf("listener activation: %w", err)
	}
}
