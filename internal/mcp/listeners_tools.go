package mcp

// MCP mirrors of the listener registration surface (listener control plane,
// slice 2). REST bodies are canonical; every rule lives in the listeners
// service. All listener refusals are pure — the service validates before
// appending — so they carry the typed pure marker and preserve keys.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/listeners"
)

func (s *Server) listenerTools() []Tool {
	return []Tool{
		s.toolListenersCreate(),
		s.toolListenersList(),
		s.toolListenersGet(),
		s.toolListenersSetPolicy(),
		s.toolListenersBindCredential(),
		s.toolListenersRetire(),
		s.toolListenersClaim(),
		s.toolListenersEnsureActivation(),
		s.toolListenerActivationsBegin(),
		s.toolListenerActivationsRecordReceipt(),
	}
}

func (s *Server) toolListenersClaim() Tool {
	return Tool{
		Name:        "listeners.claim",
		Description: "Listener-bound atomic claim: revalidates the registration, credential binding, policy revision, demand eligibility, actor authority, and listener capacity in one transaction before assigning. Callers must be the listener's currently bound principal. Supervisors use this, never the generic work-item claim.",
		Mutates:     true,
		InputSchema: schemaObject([]string{"id", "demand_event_id", "observed_policy_event_id"}, map[string]any{
			"id":                       schemaString("Listener uuid."),
			"demand_event_id":          schemaString("Durable demand event uuid (dispatch.requested)."),
			"observed_policy_event_id": schemaString("The policy revision the caller derived its snapshot under; a stale revision is a pure conflict."),
		}),
		Handler: func(ctx context.Context, actor domain.Token, raw json.RawMessage) (any, error) {
			if s.deps.Listeners == nil {
				return nil, errors.New("listeners service not configured")
			}
			var args struct {
				ID                    string `json:"id"`
				DemandEventID         string `json:"demand_event_id"`
				ObservedPolicyEventID string `json:"observed_policy_event_id"`
			}
			if err := decodeArgs(raw, &args); err != nil {
				return nil, err
			}
			id, err := parseUUID(args.ID, "id")
			if err != nil {
				return nil, err
			}
			demandEventID, err := parseUUID(args.DemandEventID, "demand_event_id")
			if err != nil {
				return nil, err
			}
			observed, err := parseUUID(args.ObservedPolicyEventID, "observed_policy_event_id")
			if err != nil {
				return nil, err
			}
			assignment, err := s.deps.Listeners.ClaimDemand(ctx, id, listeners.ClaimDemandInput{
				DemandEventID:         demandEventID,
				ObservedPolicyEventID: &observed,
				Actor:                 actor,
			})
			if err != nil {
				if mapped := listenerToolErr(err); isReplayableToolError(mapped) {
					return nil, mapped
				}
				return nil, assignmentToolErr(err, uuid.Nil)
			}
			return map[string]any{"assignment": toAssignmentDTO(assignment)}, nil
		},
	}
}

func (s *Server) toolListenersCreate() Tool {
	return Tool{
		Name:        "listeners.create",
		Description: "Register a durable listener: a stable routing address for a client endpoint that can accept assignments. Human listener-admin scope only.",
		Mutates:     true,
		InputSchema: schemaObject([]string{"name", "principal_token_id", "capabilities"}, map[string]any{
			"name":               schemaString("Operator-facing unique listener name."),
			"principal_token_id": schemaString("Stable credential uuid currently accountable for claims."),
			"provider":           schemaString("Optional routing datum (e.g. codex); never an authority grant."),
			"capabilities": map[string]any{
				"type":        "array",
				"items":       map[string]any{"type": "string"},
				"description": "Capability names this listener offers.",
			},
		}),
		Handler: func(ctx context.Context, actor domain.Token, raw json.RawMessage) (any, error) {
			if s.deps.Listeners == nil {
				return nil, errors.New("listeners service not configured")
			}
			var args struct {
				Name             string   `json:"name"`
				PrincipalTokenID string   `json:"principal_token_id"`
				Provider         string   `json:"provider"`
				Capabilities     []string `json:"capabilities"`
			}
			if err := decodeArgs(raw, &args); err != nil {
				return nil, err
			}
			principal, err := parseUUID(args.PrincipalTokenID, "principal_token_id")
			if err != nil {
				return nil, err
			}
			reg, err := s.deps.Listeners.Register(ctx, listeners.RegisterInput{
				Name: args.Name, PrincipalTokenID: principal,
				Provider: args.Provider, Capabilities: args.Capabilities, Actor: actor,
			})
			if err != nil {
				return nil, listenerToolErr(err)
			}
			return map[string]any{"listener": toListenerDTO(reg)}, nil
		},
	}
}

func (s *Server) toolListenersList() Tool {
	return Tool{
		Name:        "listeners.list",
		Description: "List listener registrations (live by default; include_retired for tombstones).",
		Mutates:     false,
		InputSchema: schemaObject(nil, map[string]any{
			"include_retired": map[string]any{"type": "boolean", "description": "Include retired registrations."},
		}),
		Handler: func(ctx context.Context, actor domain.Token, raw json.RawMessage) (any, error) {
			if s.deps.Listeners == nil {
				return nil, errors.New("listeners service not configured")
			}
			var args struct {
				IncludeRetired bool `json:"include_retired"`
			}
			if err := decodeArgs(raw, &args); err != nil {
				return nil, err
			}
			regs, err := s.deps.Listeners.List(ctx, args.IncludeRetired)
			if err != nil {
				return nil, listenerToolErr(err)
			}
			out := make([]map[string]any, 0, len(regs))
			for _, reg := range regs {
				out = append(out, toListenerDTO(reg))
			}
			return map[string]any{"listeners": out}, nil
		},
	}
}

func (s *Server) toolListenersGet() Tool {
	return Tool{
		Name:        "listeners.get",
		Description: "Read one listener registration by id or unique name.",
		Mutates:     false,
		InputSchema: schemaObject(nil, map[string]any{
			"id":   schemaString("Listener uuid."),
			"name": schemaString("Listener unique name (used when id is absent)."),
		}),
		Handler: func(ctx context.Context, actor domain.Token, raw json.RawMessage) (any, error) {
			if s.deps.Listeners == nil {
				return nil, errors.New("listeners service not configured")
			}
			var args struct {
				ID   string `json:"id"`
				Name string `json:"name"`
			}
			if err := decodeArgs(raw, &args); err != nil {
				return nil, err
			}
			var (
				reg listeners.Registration
				err error
			)
			switch {
			case args.ID != "":
				var id uuid.UUID
				id, err = parseUUID(args.ID, "id")
				if err != nil {
					return nil, err
				}
				reg, err = s.deps.Listeners.Get(ctx, id)
			case args.Name != "":
				reg, err = s.deps.Listeners.GetByName(ctx, args.Name)
			default:
				return nil, replayableToolErr(pureToolErr(fmt.Errorf("listeners: id or name is required")))
			}
			if err != nil {
				return nil, listenerToolErr(err)
			}
			return map[string]any{"listener": toListenerDTO(reg)}, nil
		},
	}
}

func (s *Server) toolListenersSetPolicy() Tool {
	return Tool{
		Name:        "listeners.set_policy",
		Description: "Replace a listener's base policy (complete replacement, never a patch). The listener's own principal may only narrow; widening requires the listener-admin scope. Pass observed_policy_event_id from your latest read; a stale revision is a pure conflict.",
		Mutates:     true,
		InputSchema: schemaObject([]string{"id", "policy"}, map[string]any{
			"id": schemaString("Listener uuid."),
			"policy": map[string]any{
				"type":        "object",
				"description": "Full-replacement policy: {projection?, predicates[], capabilities[], max_concurrent_assignments, focus}. Predicates use the normalized feed vocabulary.",
			},
			"observed_policy_event_id": schemaString("The policy_event_id the caller observed; required when a policy already exists."),
		}),
		Handler: func(ctx context.Context, actor domain.Token, raw json.RawMessage) (any, error) {
			if s.deps.Listeners == nil {
				return nil, errors.New("listeners service not configured")
			}
			var args struct {
				ID                    string           `json:"id"`
				Policy                listeners.Policy `json:"policy"`
				ObservedPolicyEventID string           `json:"observed_policy_event_id"`
			}
			if err := decodeArgs(raw, &args); err != nil {
				return nil, err
			}
			id, err := parseUUID(args.ID, "id")
			if err != nil {
				return nil, err
			}
			var observed *uuid.UUID
			if args.ObservedPolicyEventID != "" {
				parsed, err := parseUUID(args.ObservedPolicyEventID, "observed_policy_event_id")
				if err != nil {
					return nil, err
				}
				observed = &parsed
			}
			reg, err := s.deps.Listeners.SetPolicy(ctx, id, listeners.SetPolicyInput{
				Policy:                args.Policy,
				ObservedPolicyEventID: observed,
				Actor:                 actor,
			})
			if err != nil {
				return nil, listenerToolErr(err)
			}
			return map[string]any{"listener": toListenerDTO(reg)}, nil
		},
	}
}

func (s *Server) toolListenersBindCredential() Tool {
	return Tool{
		Name:        "listeners.bind_credential",
		Description: "Rotate the principal credential bound to a stable listener address. Human listener-admin scope only; held assignments keep their generation and resolve against the current binding at event time.",
		Mutates:     true,
		InputSchema: schemaObject([]string{"id", "principal_token_id"}, map[string]any{
			"id":                 schemaString("Listener uuid."),
			"principal_token_id": schemaString("New principal credential uuid."),
		}),
		Handler: func(ctx context.Context, actor domain.Token, raw json.RawMessage) (any, error) {
			if s.deps.Listeners == nil {
				return nil, errors.New("listeners service not configured")
			}
			var args struct {
				ID               string `json:"id"`
				PrincipalTokenID string `json:"principal_token_id"`
			}
			if err := decodeArgs(raw, &args); err != nil {
				return nil, err
			}
			id, err := parseUUID(args.ID, "id")
			if err != nil {
				return nil, err
			}
			principal, err := parseUUID(args.PrincipalTokenID, "principal_token_id")
			if err != nil {
				return nil, err
			}
			reg, err := s.deps.Listeners.BindCredential(ctx, id, principal, actor)
			if err != nil {
				return nil, listenerToolErr(err)
			}
			return map[string]any{"listener": toListenerDTO(reg)}, nil
		},
	}
}

func (s *Server) toolListenersRetire() Tool {
	return Tool{
		Name:        "listeners.retire",
		Description: "Tombstone a listener registration. Historical attribution keeps resolving; the address stops receiving routes.",
		Mutates:     true,
		InputSchema: schemaObject([]string{"id"}, map[string]any{
			"id":     schemaString("Listener uuid."),
			"reason": schemaString("Optional reason recorded with the retirement."),
		}),
		Handler: func(ctx context.Context, actor domain.Token, raw json.RawMessage) (any, error) {
			if s.deps.Listeners == nil {
				return nil, errors.New("listeners service not configured")
			}
			var args struct {
				ID     string `json:"id"`
				Reason string `json:"reason"`
			}
			if err := decodeArgs(raw, &args); err != nil {
				return nil, err
			}
			id, err := parseUUID(args.ID, "id")
			if err != nil {
				return nil, err
			}
			reg, err := s.deps.Listeners.Retire(ctx, id, args.Reason, actor)
			if err != nil {
				return nil, listenerToolErr(err)
			}
			return map[string]any{"listener": toListenerDTO(reg)}, nil
		},
	}
}

func toListenerDTO(reg listeners.Registration) map[string]any {
	out := map[string]any{
		"id":                         reg.ID.String(),
		"name":                       reg.Name,
		"principal_token_id":         reg.PrincipalTokenID.String(),
		"provider":                   reg.Provider,
		"capabilities":               reg.Capabilities,
		"max_concurrent_assignments": reg.MaxConcurrentAssignments,
		"created_at":                 reg.CreatedAt.UTC().Format(time.RFC3339Nano),
		"updated_at":                 reg.UpdatedAt.UTC().Format(time.RFC3339Nano),
	}
	if reg.Policy != nil {
		out["policy"] = reg.Policy
		out["policy_fingerprint"] = reg.PolicyFingerprint
		if reg.PolicyEventID != nil {
			out["policy_event_id"] = reg.PolicyEventID.String()
		}
	}
	if reg.RetiredAt != nil {
		out["retired_at"] = reg.RetiredAt.UTC().Format(time.RFC3339Nano)
	}
	return out
}

// listenerToolErr: every listener-service refusal is pure — validation
// precedes any append and every error path rolls back — so the key-
// preserving classification is uniform here.
func listenerToolErr(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, listeners.ErrNotFound):
		return replayableToolErr(notFoundToolError{msg: "listener_not_found: " + err.Error()})
	case errors.Is(err, listeners.ErrNameTaken),
		errors.Is(err, listeners.ErrRetired),
		errors.Is(err, listeners.ErrStalePolicy),
		errors.Is(err, listeners.ErrNotAuthorized),
		errors.Is(err, listeners.ErrInvalidPolicy),
		errors.Is(err, listeners.ErrInvalidRequest),
		errors.Is(err, listeners.ErrDemandNotEligible),
		errors.Is(err, listeners.ErrListenerAtCapacity):
		return replayableToolErr(pureToolErr(err))
	default:
		return err
	}
}
