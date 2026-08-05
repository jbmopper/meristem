package listeners

// Eligible-demand reduction (docs/listener-control-plane.md; LCP2-B1). A
// listener policy is an owner-visible listening contract over DEMAND, not a
// feed filter over general activity. This file defines the one normalized
// reduction both the resolver (slice 2) and the supervisor (slice 3) use:
//
//   - What counts as demand is the pinned demand projection's kind set —
//     ordinary chatter (agent.status, review flow, ...) is never eligible,
//     whatever the predicates say.
//   - Predicates evaluate against the DemandEnvelope, not the raw event row.
//     In particular the actor predicate matches the demand's ORIGINATING
//     principal (the creator of the work item whose demand this is), never
//     the event author — dispatch.requested is system-authored, so matching
//     authorship would make "listen to Fable" unsatisfiable.
//   - The envelope's origin and lineage are derived server-side from the
//     canonical work-item projections at evaluation time. They are never
//     caller-asserted: a producer cannot forge its way into a listener's
//     lens by claiming someone else's origin.

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/feed"
)

// DemandProjection is the only projection a listener base policy may select
// in this release: the immutable rootstock dispatch demand lane. Its kind set
// is pinned here (and drift-tested against the seeded projection definition)
// so eligibility never silently follows a mutated projection definition.
const DemandProjection = "dispatch"

// DemandProjectionKinds is the pinned kind set of DemandProjection. Only
// these kinds can ever be eligible demand.
var DemandProjectionKinds = []string{domain.EventDispatchRequested}

// DemandEnvelope is the normalized routing view of one demand event: the
// work item the demand is for, its tree lineage, the originating principal,
// and the demand kind. It is the ONLY input predicates evaluate against.
type DemandEnvelope struct {
	// Capability the producer is requesting.
	Capability string
	// EventKind of the demand event (must be in DemandProjectionKinds).
	EventKind string
	// WorkItemID the demand is anchored on.
	WorkItemID uuid.UUID
	// Lineage is WorkItemID plus every ancestor up to its root(s); the
	// work_item_tree predicate tests membership here.
	Lineage []uuid.UUID
	// OriginTokenID is the originating principal of the demand — the creator
	// of the work item — NOT the (system) author of the demand event.
	OriginTokenID uuid.UUID
}

// EligibleDemand is the pure reduction: does envelope match this listener's
// listening contract? policy may be nil (a registration whose base policy the
// admin has not initialized yet listens to all eligible demand for its
// registered capabilities). Fail-closed: unknown predicate kinds refuse.
func EligibleDemand(policy *Policy, registeredCapabilities []string, env DemandEnvelope) bool {
	if !slices.Contains(DemandProjectionKinds, env.EventKind) {
		return false
	}
	offered := registeredCapabilities
	if policy != nil {
		offered = policy.Capabilities
	}
	if !slices.Contains(offered, env.Capability) {
		return false
	}
	if policy == nil {
		return true
	}
	for _, predicate := range policy.Predicates {
		if !predicateMatchesDemand(predicate, env) {
			return false
		}
	}
	return true
}

// predicateMatchesDemand maps each persisted feed-vocabulary predicate onto
// the demand envelope. Every arm is fail-closed: a predicate this reduction
// does not understand refuses the demand rather than admitting it.
func predicateMatchesDemand(w PredicateWire, env DemandEnvelope) bool {
	switch feed.PredicateKind(w.Kind) {
	case feed.PredicateActor:
		for _, raw := range w.TokenIDs {
			if id, err := uuid.Parse(raw); err == nil && id == env.OriginTokenID {
				return true
			}
		}
		return false
	case feed.PredicateExcludeActor:
		id, err := uuid.Parse(w.TokenID)
		return err == nil && id != env.OriginTokenID
	case feed.PredicateWorkItem:
		id, err := uuid.Parse(w.WorkItemID)
		return err == nil && id == env.WorkItemID
	case feed.PredicateWorkItemTree:
		id, err := uuid.Parse(w.WorkItemID)
		return err == nil && slices.Contains(env.Lineage, id)
	case feed.PredicateKindInclude:
		return slices.Contains(w.EventKinds, env.EventKind)
	case feed.PredicateKindExclude:
		return !slices.Contains(w.EventKinds, env.EventKind)
	default:
		// assigned_or_addressed is refused at normalization; anything else
		// is vocabulary this reduction has not been taught. Fail closed.
		return false
	}
}

// DemandInput names one demand for resolution. Origin and lineage are NOT
// inputs — the service derives them from the canonical projections.
type DemandInput struct {
	Capability string
	EventKind  string
	WorkItemID uuid.UUID
}

// ResolveForDemand is the deterministic router seam (slice-2 exit): producers
// present a demand — capability plus the demand event's kind and work item —
// never a bearer UUID. Selection is a pure reduction: live registrations
// whose listening contract admits the envelope, ordered by (created_at, id);
// the first is the route. Eligibility beyond the contract (scopes, capacity,
// review state) is re-validated at claim time, so this choice can be
// optimistic.
func (s *Service) ResolveForDemand(ctx context.Context, in DemandInput) (Registration, error) {
	capability := strings.TrimSpace(in.Capability)
	if capability == "" {
		return Registration{}, fmt.Errorf("%w: capability is required", ErrInvalidRequest)
	}
	kind := strings.TrimSpace(in.EventKind)
	if !slices.Contains(DemandProjectionKinds, kind) {
		return Registration{}, fmt.Errorf("%w: %q is not an eligible demand kind", ErrInvalidRequest, in.EventKind)
	}
	if in.WorkItemID == uuid.Nil {
		return Registration{}, fmt.Errorf("%w: work_item_id is required", ErrInvalidRequest)
	}
	env, err := s.demandEnvelope(ctx, capability, kind, in.WorkItemID)
	if err != nil {
		return Registration{}, err
	}
	regs, err := s.List(ctx, false)
	if err != nil {
		return Registration{}, err
	}
	for _, reg := range regs {
		if EligibleDemand(reg.Policy, reg.Capabilities, env) {
			return reg, nil
		}
	}
	return Registration{}, fmt.Errorf("%w: no live listener's contract admits capability %q for this demand", ErrNotFound, capability)
}

// demandEnvelope resolves the envelope's origin (work item creator) and tree
// lineage from the canonical projections.
func (s *Service) demandEnvelope(ctx context.Context, capability, kind string, workItemID uuid.UUID) (DemandEnvelope, error) {
	var createdBy *uuid.UUID
	err := s.pool.QueryRow(ctx, `SELECT created_by FROM work_items WHERE id=$1`, workItemID).Scan(&createdBy)
	if errors.Is(err, pgx.ErrNoRows) {
		return DemandEnvelope{}, fmt.Errorf("%w: demand work item %s", ErrNotFound, workItemID)
	}
	if err != nil {
		return DemandEnvelope{}, fmt.Errorf("listeners: resolve demand origin: %w", err)
	}
	env := DemandEnvelope{Capability: capability, EventKind: kind, WorkItemID: workItemID}
	if createdBy != nil {
		env.OriginTokenID = *createdBy
	}
	rows, err := s.pool.Query(ctx, `
		WITH RECURSIVE lineage(id) AS (
			SELECT $1::uuid
			UNION
			SELECT wir.parent_id
			FROM work_item_relations wir
			JOIN lineage l ON wir.child_id = l.id
		)
		SELECT id FROM lineage`, workItemID)
	if err != nil {
		return DemandEnvelope{}, fmt.Errorf("listeners: resolve demand lineage: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return DemandEnvelope{}, err
		}
		env.Lineage = append(env.Lineage, id)
	}
	return env, rows.Err()
}
