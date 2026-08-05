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

	"github.com/jbmopper/meristem/internal/access"
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
	env, err := s.demandEnvelope(ctx, s.pool, demandEventID)
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

// DemandCandidate is one open eligible demand as the supervisor's snapshot
// sees it: the durable demand event plus its normalized envelope, ordered by
// the design's deterministic candidate order (dispatch_event_seq,
// work_item_id).
type DemandCandidate struct {
	DemandEventID  uuid.UUID
	DemandEventSeq int64
	Envelope       DemandEnvelope
}

// ListDemandCandidates is the snapshot half of mint-before-snapshot (slice
// 3): every OPEN eligible demand for this listener's stored policy, in
// deterministic candidate order. Openness mirrors the claim gate — the work
// item is nonterminal, not blocked or awaiting approval, not human-review
// blocked, and carries no unexpired assignment — so a candidate returned
// here is claimable unless a racer wins first (the claim conflict reducer
// collapses that race). Per work item only the LATEST demand event counts.
// Eligibility is evaluated against the STORED policy server-side; a
// policy-less or retired registration has no candidates. A malformed durable
// demand event (missing capability or origin) is skipped, never guessed at.
//
// The reduction ALSO applies the calling actor's own object authority
// (LCP3-R1-B2): a candidate the actor could not claim — no write authority
// over the work item's tree — is ABSENT from the listing, not merely
// unclaimable later. A broad or misconfigured base policy therefore cannot
// turn a tree-scoped principal into a portfolio-wide demand enumerator; the
// listener-bound claim revalidates the same authority atomically.
func (s *Service) ListDemandCandidates(ctx context.Context, listenerID uuid.UUID, actor domain.Token) ([]DemandCandidate, error) {
	reg, err := scanRegistration(ctx, s.pool, listenerID)
	if err != nil {
		return nil, err
	}
	if reg.RetiredAt != nil || reg.Policy == nil {
		return nil, nil
	}
	rows, err := s.pool.Query(ctx, `
		SELECT d.id, d.seq FROM (
			SELECT DISTINCT ON (e.subject_id) e.id, e.seq, e.subject_id
			FROM events e
			JOIN work_items wi ON wi.id = e.subject_id
			LEFT JOIN work_item_assignment_state a ON a.work_item_id = wi.id
			WHERE e.kind = $1 AND e.subject_kind = $2
			  AND wi.state <> ALL($3::text[])
			  AND wi.human_review_status <> $4
			  AND (a.holder_token_id IS NULL OR a.expires_at <= clock_timestamp())
			ORDER BY e.subject_id, e.seq DESC
		) d
		ORDER BY d.seq, d.subject_id`,
		domain.EventDispatchRequested, domain.SubjectWorkItem,
		[]string{
			string(domain.WorkItemDone), string(domain.WorkItemFailed), string(domain.WorkItemCanceled),
			string(domain.WorkItemBlocked), string(domain.WorkItemAwaitingApproval),
		},
		string(domain.HumanReviewBlocked),
	)
	if err != nil {
		return nil, fmt.Errorf("listeners: list demand candidates: %w", err)
	}
	defer rows.Close()
	type row struct {
		id  uuid.UUID
		seq int64
	}
	var candidates []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.seq); err != nil {
			return nil, err
		}
		candidates = append(candidates, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	var out []DemandCandidate
	for _, c := range candidates {
		env, err := s.demandEnvelope(ctx, s.pool, c.id)
		if err != nil {
			if errors.Is(err, ErrInvalidRequest) || errors.Is(err, ErrNotFound) {
				continue
			}
			return nil, err
		}
		if !EligibleDemand(reg.Policy, env) {
			continue
		}
		if err := s.access.CanWriteWorkItem(ctx, actor, env.WorkItemID); err != nil {
			if errors.Is(err, access.ErrDenied) {
				continue
			}
			return nil, err
		}
		out = append(out, DemandCandidate{DemandEventID: c.id, DemandEventSeq: c.seq, Envelope: env})
	}
	return out, nil
}

// demandEnvelope loads the durable demand event and normalizes it. The
// capability and originating principal come from the event payload — written
// by the dispatch reconciler, never by the resolving caller — and lineage
// comes from the canonical relations projection. Every gap fails closed: a
// missing event, a non-demand kind, or a payload without routing metadata
// refuses rather than guessing.
func (s *Service) demandEnvelope(ctx context.Context, q queryer, demandEventID uuid.UUID) (DemandEnvelope, error) {
	var (
		kind        string
		subjectKind string
		subjectID   uuid.UUID
		payloadRaw  []byte
	)
	err := q.QueryRow(ctx, `SELECT kind, subject_kind, subject_id, payload FROM events WHERE id=$1`, demandEventID).
		Scan(&kind, &subjectKind, &subjectID, &payloadRaw)
	if errors.Is(err, pgx.ErrNoRows) {
		return DemandEnvelope{}, fmt.Errorf("%w: demand event %s", ErrNotFound, demandEventID)
	}
	if err != nil {
		return DemandEnvelope{}, fmt.Errorf("listeners: load demand event: %w", err)
	}
	if !slices.Contains(DemandProjectionKinds, kind) {
		return DemandEnvelope{}, fmt.Errorf("%w: event %s is %q, not an eligible demand kind", ErrInvalidRequest, demandEventID, kind)
	}
	if subjectKind != domain.SubjectWorkItem {
		return DemandEnvelope{}, fmt.Errorf("%w: demand event %s has subject kind %q, not a work item", ErrInvalidRequest, demandEventID, subjectKind)
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
	// The payload's work_item_id is redundant routing metadata; if present it
	// must AGREE with the event's subject — otherwise a malformed durable
	// event could evaluate policy against a different tree than the event is
	// actually about.
	if payload.WorkItemID != uuid.Nil && payload.WorkItemID != subjectID {
		return DemandEnvelope{}, fmt.Errorf("%w: demand event %s names work item %s but is about %s", ErrInvalidRequest, demandEventID, payload.WorkItemID, subjectID)
	}
	workItemID := subjectID
	env := DemandEnvelope{
		Capability:    strings.TrimSpace(payload.Capability),
		EventKind:     kind,
		WorkItemID:    workItemID,
		OriginTokenID: payload.OriginTokenID,
	}
	rows, err := q.Query(ctx, `
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
