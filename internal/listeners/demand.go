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
//     principal — the last non-system principal that advanced the work item,
//     recorded on the dispatch event by the reconciler — never the event
//     author: dispatch.requested is system-authored, so matching authorship
//     would make "listen to Fable" unsatisfiable.
//   - Resolution binds to a DURABLE demand event: capability and origin come
//     from that event's payload (written by the reconciler), lineage from the
//     canonical relations projection. Nothing is caller-asserted, so a
//     producer cannot forge its way into a listener's lens.

import (
	"context"
	"encoding/json"
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
	// OriginTokenID is the originating principal of the demand — the last
	// non-system principal that advanced the work item, as recorded on the
	// demand event — NOT the (system) author of the demand event.
	OriginTokenID uuid.UUID
}

// EligibleDemand is the pure reduction: does envelope match this listener's
// listening contract? A registration with NO base policy has no contract yet
// and is not routable (LCP2-R2-B3) — administration establishes the intended
// lens before any demand can reach the listener, so a listener meant for one
// actor or topic can never receive broad demand in the gap between
// registration and its first policy. Fail-closed throughout: unknown
// predicate kinds refuse. (A normalized policy always carries its effective
// capability set — an empty submission normalizes to the registered set — so
// the policy alone is the whole contract.)
func EligibleDemand(policy *Policy, env DemandEnvelope) bool {
	if policy == nil {
		return false
	}
	if !slices.Contains(DemandProjectionKinds, env.EventKind) {
		return false
	}
	if !slices.Contains(policy.Capabilities, env.Capability) {
		return false
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

// ResolveForDemand is the deterministic router seam (slice-2 exit): producers
// present the id of a DURABLE demand event, never a bearer UUID and never
// caller-asserted routing fields (LCP2-R2-B2). The service loads the event,
// verifies its kind is in the demand lane, and derives capability, origin,
// work item, and lineage from its event-backed payload and the canonical
// relations projection — so a producer cannot forge its way into a listener's
// lens by claiming someone else's capability or origin. Selection is a pure
// reduction: live registrations whose listening contract admits the envelope,
// ordered by (created_at, id); the first is the route. Eligibility beyond the
// contract (scopes, capacity, review state) is re-validated at claim time, so
// this choice can be optimistic.
func (s *Service) ResolveForDemand(ctx context.Context, demandEventID uuid.UUID) (Registration, error) {
	if demandEventID == uuid.Nil {
		return Registration{}, fmt.Errorf("%w: demand event id is required", ErrInvalidRequest)
	}
	env, err := s.demandEnvelope(ctx, demandEventID)
	if err != nil {
		return Registration{}, err
	}
	regs, err := s.List(ctx, false)
	if err != nil {
		return Registration{}, err
	}
	for _, reg := range regs {
		if EligibleDemand(reg.Policy, env) {
			return reg, nil
		}
	}
	return Registration{}, fmt.Errorf("%w: no live listener's contract admits capability %q for demand %s", ErrNotFound, env.Capability, demandEventID)
}

// demandEnvelope loads the durable demand event and normalizes it. The
// capability and originating principal come from the event payload — written
// by the dispatch reconciler, never by the resolving caller — and lineage
// comes from the canonical relations projection. Every gap fails closed: a
// missing event, a non-demand kind, or a payload without routing metadata
// refuses rather than guessing.
func (s *Service) demandEnvelope(ctx context.Context, demandEventID uuid.UUID) (DemandEnvelope, error) {
	var (
		kind       string
		subjectID  uuid.UUID
		payloadRaw []byte
	)
	err := s.pool.QueryRow(ctx, `SELECT kind, subject_id, payload FROM events WHERE id=$1`, demandEventID).
		Scan(&kind, &subjectID, &payloadRaw)
	if errors.Is(err, pgx.ErrNoRows) {
		return DemandEnvelope{}, fmt.Errorf("%w: demand event %s", ErrNotFound, demandEventID)
	}
	if err != nil {
		return DemandEnvelope{}, fmt.Errorf("listeners: load demand event: %w", err)
	}
	if !slices.Contains(DemandProjectionKinds, kind) {
		return DemandEnvelope{}, fmt.Errorf("%w: event %s is %q, not an eligible demand kind", ErrInvalidRequest, demandEventID, kind)
	}
	var payload struct {
		Capability    string    `json:"capability"`
		OriginTokenID uuid.UUID `json:"origin_token_id"`
		WorkItemID    uuid.UUID `json:"work_item_id"`
	}
	if err := json.Unmarshal(payloadRaw, &payload); err != nil {
		return DemandEnvelope{}, fmt.Errorf("listeners: decode demand payload: %w", err)
	}
	if strings.TrimSpace(payload.Capability) == "" {
		return DemandEnvelope{}, fmt.Errorf("%w: demand event %s carries no capability", ErrInvalidRequest, demandEventID)
	}
	if payload.OriginTokenID == uuid.Nil {
		return DemandEnvelope{}, fmt.Errorf("%w: demand event %s carries no originating principal", ErrInvalidRequest, demandEventID)
	}
	workItemID := payload.WorkItemID
	if workItemID == uuid.Nil {
		workItemID = subjectID
	}
	env := DemandEnvelope{
		Capability:    strings.TrimSpace(payload.Capability),
		EventKind:     kind,
		WorkItemID:    workItemID,
		OriginTokenID: payload.OriginTokenID,
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
