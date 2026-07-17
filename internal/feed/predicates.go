package feed

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/domain"
)

// PredicateKind names one normalized, transport-independent feed predicate.
// New read surfaces translate their wire arguments into this vocabulary; the
// feed query paths evaluate the resulting contract without transport-specific
// branches.
type PredicateKind string

const (
	PredicateAssignedOrAddressed PredicateKind = "assigned_or_addressed"

	// PredicateExcludeActor removes events authored by TokenID from the view.
	// Directed signals outrank authorship quieting: an event the excluded
	// actor explicitly addressed to a DIFFERENT token (assignment controls,
	// structured addressee fields, terminal-handback bindings) survives, so
	// excluding a chatty coordinator can never swallow the caller's own
	// assignment or handback wake. Events the excluded actor addressed to
	// itself are removed with the rest of its authorship.
	PredicateExcludeActor PredicateKind = "exclude_actor"
)

var (
	ErrUnknownPredicate = errors.New("feed: unknown predicate")
	ErrInvalidPredicate = errors.New("feed: invalid predicate")
)

// Predicate is one reducer in a ReadFilter. Predicates are ANDed; any OR
// relationship belongs inside a named predicate's definition. TokenID is
// explicit identity, never a name or a value inferred from prose.
type Predicate struct {
	Kind    PredicateKind
	TokenID uuid.UUID
}

// BatchReducer applies an authorization or policy reduction to a candidate
// batch. It must only remove items. Errors fail the read closed.
type BatchReducer func(context.Context, []Item) ([]Item, error)

// ReadFilter is the normalized contract shared by snapshot, long-poll, and
// streaming feed reads. Projection selects event kinds; Predicates only narrow
// that selected view. Reduce is evaluated inside each scan batch so filtered
// traffic cannot satisfy a long poll or consume a snapshot limit.
type ReadFilter struct {
	Projection *ProjectionFilter
	Predicates []Predicate
	Reduce     BatchReducer
}

// NormalizeReadFilter validates and canonicalizes the transport-independent
// predicate set. Projection definitions are normalized when persisted, so this
// function deliberately does not reinterpret their already-versioned shape.
func NormalizeReadFilter(in ReadFilter) (ReadFilter, error) {
	out := ReadFilter{Projection: in.Projection, Reduce: in.Reduce}
	seen := make(map[string]bool, len(in.Predicates))
	for _, predicate := range in.Predicates {
		switch predicate.Kind {
		case PredicateAssignedOrAddressed, PredicateExcludeActor:
			if predicate.TokenID == uuid.Nil {
				return ReadFilter{}, fmt.Errorf("%w: %s requires token identity", ErrInvalidPredicate, predicate.Kind)
			}
		default:
			return ReadFilter{}, fmt.Errorf("%w: %s", ErrUnknownPredicate, predicate.Kind)
		}
		key := string(predicate.Kind) + ":" + predicate.TokenID.String()
		if seen[key] {
			continue
		}
		seen[key] = true
		out.Predicates = append(out.Predicates, predicate)
	}
	slices.SortFunc(out.Predicates, func(a, b Predicate) int {
		if a.Kind < b.Kind {
			return -1
		}
		if a.Kind > b.Kind {
			return 1
		}
		return strings.Compare(a.TokenID.String(), b.TokenID.String())
	})
	return out, nil
}

func (f ReadFilter) queryKinds() []string {
	if f.Projection == nil {
		kinds := slices.Clone(IncludedKinds)
		for _, predicate := range f.Predicates {
			if predicate.Kind == PredicateAssignedOrAddressed {
				// Assignment lifecycle remains audit-only/admin for the default
				// feed and persisted projections. The actor-addressed runtime lane
				// is the narrow exception that wakes an idle claimant on assign or
				// release without reclassifying either event kind.
				kinds = append(kinds, domain.EventWorkItemAssigned, domain.EventWorkItemAssignmentReleased)
				break
			}
		}
		return kinds
	}
	return f.Projection.QueryKinds()
}

func (f ReadFilter) matchesProjection(item Item) bool {
	return f.Projection == nil || f.Projection.Matches(item)
}

// matchingItems evaluates all normalized predicates over a query batch. The
// assignment lookup is set-shaped, so one page costs one projection read
// rather than one query per event or work item.
func (s *Service) matchingItems(ctx context.Context, filter ReadFilter, items []Item) ([]bool, error) {
	matches := make([]bool, len(items))
	for i := range matches {
		matches[i] = filter.matchesProjection(items[i])
	}
	for _, predicate := range filter.Predicates {
		switch predicate.Kind {
		case PredicateAssignedOrAddressed:
			addresses := make([]explicitAddress, len(items))
			for i, item := range items {
				addresses[i] = parseExplicitAddressee(item)
			}
			terminalAddresses, err := s.terminalAddressedEvents(ctx, predicate.TokenID, items, matches, addresses)
			if err != nil {
				return nil, err
			}
			assigned, err := s.assignedWorkItems(ctx, predicate.TokenID, items, matches, addresses)
			if err != nil {
				return nil, err
			}
			for i, item := range items {
				if !matches[i] {
					continue
				}
				if addresses[i].invalid {
					matches[i] = false
					continue
				}
				anchors := WorkItemAnchors(item)
				addressed := (addresses[i].present && addresses[i].tokenID == predicate.TokenID) || terminalAddresses[item.EventID]
				if assignmentControlKind(item.Kind) {
					// Control events target their payload assignee exactly. Never
					// reinterpret a stale A control event as activity for the item's
					// later/current holder B.
					matches[i] = addressed
					continue
				}
				if addressed {
					continue
				}
				matches[i] = false
				for _, id := range anchors {
					if assigned[id] {
						matches[i] = true
						break
					}
				}
			}
		case PredicateExcludeActor:
			// Only-removes: this case may clear matches, never set them. The
			// carve-outs below keep directed signals (explicit addressee or
			// terminal-handback binding to a token other than the excluded
			// one); a malformed address never rescues an event from exclusion.
			authored := make([]bool, len(items))
			directed := make([]bool, len(items))
			undirected := make([]uuid.UUID, 0, len(items))
			for i, item := range items {
				if !matches[i] || item.ActorTokenID == nil || *item.ActorTokenID != predicate.TokenID {
					continue
				}
				authored[i] = true
				address := parseExplicitAddressee(item)
				directed[i] = !address.invalid && address.present && address.tokenID != predicate.TokenID
				if !address.invalid && !address.present {
					undirected = append(undirected, item.EventID)
				}
			}
			handback, err := s.terminalAddressedElsewhere(ctx, predicate.TokenID, undirected)
			if err != nil {
				return nil, err
			}
			for i, item := range items {
				if authored[i] && !directed[i] && !handback[item.EventID] {
					matches[i] = false
				}
			}
		default:
			return nil, fmt.Errorf("%w: %s", ErrUnknownPredicate, predicate.Kind)
		}
	}
	if filter.Reduce != nil {
		candidates := make([]Item, 0, len(items))
		for i, item := range items {
			if matches[i] {
				candidates = append(candidates, item)
			}
		}
		reduced, err := filter.Reduce(ctx, candidates)
		if err != nil {
			return nil, fmt.Errorf("feed: reduce candidate batch: %w", err)
		}
		allowed := make(map[uuid.UUID]bool, len(reduced))
		for _, item := range reduced {
			allowed[item.EventID] = true
		}
		for i, item := range items {
			matches[i] = matches[i] && allowed[item.EventID]
		}
	}
	return matches, nil
}

// terminalAddressedEvents resolves the one terminal transition retained for a
// former assignment holder. The projection binds the holder to its exact
// state_event_id; matching event ids rather than work-item anchors prevents a
// terminal handback from widening the item's entire history.
func (s *Service) terminalAddressedEvents(ctx context.Context, tokenID uuid.UUID, items []Item, candidates []bool, addresses []explicitAddress) (map[uuid.UUID]bool, error) {
	eventIDs := make([]uuid.UUID, 0, len(items))
	for i, item := range items {
		if !candidates[i] || addresses[i].invalid || assignmentControlKind(item.Kind) ||
			(addresses[i].present && addresses[i].tokenID == tokenID) {
			continue
		}
		eventIDs = append(eventIDs, item.EventID)
	}
	matched := make(map[uuid.UUID]bool, len(eventIDs))
	if len(eventIDs) == 0 {
		return matched, nil
	}
	rows, err := s.pool.Query(ctx, `
		SELECT state_event_id
		FROM work_item_assignment_state
		WHERE terminal_addressee_token_id = $1
		  AND terminal_state IS NOT NULL
		  AND state_event_id = ANY($2::uuid[])
	`, tokenID, eventIDs)
	if err != nil {
		return nil, fmt.Errorf("feed: resolve terminal addressed events: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var eventID uuid.UUID
		if err := rows.Scan(&eventID); err != nil {
			return nil, fmt.Errorf("feed: scan terminal addressed event: %w", err)
		}
		matched[eventID] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("feed: resolve terminal addressed events: %w", err)
	}
	return matched, nil
}

// terminalAddressedElsewhere reports which of the given events are terminal
// transitions the handback projection binds to a former holder OTHER than the
// excluded token. Those events are directed wake signals for that holder, so
// actor exclusion must not remove them; a binding to the excluded token itself
// stays removable (self-quieting covers self-directed signals).
func (s *Service) terminalAddressedElsewhere(ctx context.Context, excluded uuid.UUID, eventIDs []uuid.UUID) (map[uuid.UUID]bool, error) {
	matched := make(map[uuid.UUID]bool, len(eventIDs))
	if len(eventIDs) == 0 {
		return matched, nil
	}
	rows, err := s.pool.Query(ctx, `
		SELECT state_event_id
		FROM work_item_assignment_state
		WHERE terminal_addressee_token_id IS NOT NULL
		  AND terminal_addressee_token_id <> $1
		  AND terminal_state IS NOT NULL
		  AND state_event_id = ANY($2::uuid[])
	`, excluded, eventIDs)
	if err != nil {
		return nil, fmt.Errorf("feed: resolve terminal handbacks under actor exclusion: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var eventID uuid.UUID
		if err := rows.Scan(&eventID); err != nil {
			return nil, fmt.Errorf("feed: scan terminal handback under actor exclusion: %w", err)
		}
		matched[eventID] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("feed: resolve terminal handbacks under actor exclusion: %w", err)
	}
	return matched, nil
}

func (s *Service) assignedWorkItems(ctx context.Context, tokenID uuid.UUID, items []Item, candidates []bool, addresses []explicitAddress) (map[uuid.UUID]bool, error) {
	ids := candidateAnchorIDs(items, candidates, func(i int, item Item) bool {
		return assignmentControlKind(item.Kind) || addresses[i].invalid || (addresses[i].present && addresses[i].tokenID == tokenID)
	})
	assigned := make(map[uuid.UUID]bool, len(ids))
	if len(ids) == 0 {
		return assigned, nil
	}
	rows, err := s.pool.Query(ctx, `
		SELECT work_item_id
		FROM work_item_assignment_state
		WHERE holder_token_id = $1
		  AND assignment_event_id IS NOT NULL
		  AND terminal_state IS NULL
		  AND work_item_id = ANY($2::uuid[])
	`, tokenID, ids)
	if err != nil {
		return nil, fmt.Errorf("feed: resolve assigned work items: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("feed: scan assigned work item: %w", err)
		}
		assigned[id] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("feed: resolve assigned work items: %w", err)
	}
	return assigned, nil
}

func candidateAnchorIDs(items []Item, candidates []bool, skip func(int, Item) bool) []uuid.UUID {
	ids := make([]uuid.UUID, 0, len(items))
	seen := make(map[uuid.UUID]bool)
	for i, item := range items {
		if !candidates[i] || (skip != nil && skip(i, item)) {
			continue
		}
		for _, id := range WorkItemAnchors(item) {
			if id == uuid.Nil || seen[id] {
				continue
			}
			seen[id] = true
			ids = append(ids, id)
		}
	}
	return ids
}

func assignmentControlKind(kind string) bool {
	return kind == domain.EventWorkItemAssigned || kind == domain.EventWorkItemAssignmentReleased
}

// ExplicitAddresseeTokenID returns only the canonical structured identity
// locations. A native event may carry payload.addressee_token_id; the generic
// work_item.event_appended envelope carries the same field under payload.inner.
// Ambiguous/conflicting identities and prose fields such as addressed_to fail
// closed.
func ExplicitAddresseeTokenID(item Item) uuid.UUID {
	address := parseExplicitAddressee(item)
	if address.invalid || !address.present {
		return uuid.Nil
	}
	return address.tokenID
}

type explicitAddress struct {
	tokenID uuid.UUID
	present bool
	invalid bool
}

func parseExplicitAddressee(item Item) explicitAddress {
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(item.Payload, &envelope); err != nil || envelope == nil {
		return explicitAddress{invalid: true}
	}
	var identities []uuid.UUID
	appendIdentity := func(raw json.RawMessage) bool {
		if len(raw) == 0 {
			return true
		}
		var id uuid.UUID
		if err := json.Unmarshal(raw, &id); err != nil || id == uuid.Nil {
			return false
		}
		identities = append(identities, id)
		return true
	}
	if assignmentControlKind(item.Kind) {
		// Control events are addressed by their schema's exact assignee field;
		// a generic addressee field must not forge or override that epoch.
		if _, ok := envelope["addressee_token_id"]; ok {
			return explicitAddress{invalid: true}
		}
		raw, ok := envelope["assignee_token_id"]
		if !ok {
			return explicitAddress{}
		}
		if !appendIdentity(raw) {
			return explicitAddress{invalid: true}
		}
	} else if item.Kind == domain.EventWorkItemEventAppended {
		// The generic event wrapper owns only payload.inner; top-level fields
		// belong to the envelope and are never caller-provided addressing.
		if _, ok := envelope["addressee_token_id"]; ok {
			return explicitAddress{invalid: true}
		}
		if raw, ok := envelope["inner"]; ok {
			var inner map[string]json.RawMessage
			if err := json.Unmarshal(raw, &inner); err == nil && inner != nil {
				if addressed, ok := inner["addressee_token_id"]; ok && !appendIdentity(addressed) {
					return explicitAddress{invalid: true}
				}
			}
		}
	} else if raw, ok := envelope["addressee_token_id"]; ok && !appendIdentity(raw) {
		return explicitAddress{invalid: true}
	}
	if len(identities) == 0 {
		return explicitAddress{}
	}
	identity := identities[0]
	for _, candidate := range identities[1:] {
		if candidate != identity {
			return explicitAddress{invalid: true}
		}
	}
	return explicitAddress{tokenID: identity, present: true}
}

// WorkItemAnchors maps a feed item to the work items whose visibility it can
// describe. Assignment predicates and authorization use this exact mapping so
// relation and non-work-item subject events cannot drift between reducers.
func WorkItemAnchors(item Item) []uuid.UUID {
	switch item.Kind {
	case domain.EventWorkItemRelationAdded:
		return relationWorkItemIDs(item)
	case domain.EventConvergenceChecksProposed,
		domain.EventConvergenceVerdictRecorded:
		return []uuid.UUID{item.SubjectID}
	case domain.EventMessageCaptured,
		domain.EventSignalReceived,
		domain.EventEscalationRequested,
		domain.EventSubactorGrantRequested,
		domain.EventSubactorGrantGranted,
		domain.EventSubactorGrantDenied,
		domain.EventSubactorGrantEscalated,
		domain.EventCultivarActivationRequested,
		domain.EventCultivarActivationGranted,
		domain.EventCultivarActivationDenied,
		domain.EventCultivarActivationEscalated,
		domain.EventApprovalCreated,
		domain.EventApprovalDecided,
		domain.EventApprovalExpired,
		domain.EventHTTPConnectorActionRequested,
		domain.EventHTTPConnectorActionApproved,
		domain.EventHTTPConnectorActionSent:
		return payloadWorkItemIDs(item)
	case domain.EventTropismDefined,
		domain.EventCultivarDefined,
		domain.EventProjectionDefined:
		return nil
	default:
		if item.SubjectKind == domain.SubjectWorkItem {
			return []uuid.UUID{item.SubjectID}
		}
		return nil
	}
}

func payloadWorkItemIDs(item Item) []uuid.UUID {
	var payload struct {
		WorkItemID      uuid.UUID `json:"work_item_id"`
		HumanWorkItemID uuid.UUID `json:"human_work_item_id"`
	}
	var ids []uuid.UUID
	if err := json.Unmarshal(item.Payload, &payload); err == nil {
		if payload.WorkItemID != uuid.Nil {
			ids = append(ids, payload.WorkItemID)
		}
		if payload.HumanWorkItemID != uuid.Nil {
			ids = append(ids, payload.HumanWorkItemID)
		}
	}
	return ids
}

func relationWorkItemIDs(item Item) []uuid.UUID {
	ids := []uuid.UUID{item.SubjectID}
	var payload struct {
		ParentID uuid.UUID `json:"parent_id"`
		ChildID  uuid.UUID `json:"child_id"`
	}
	if err := json.Unmarshal(item.Payload, &payload); err == nil {
		if payload.ParentID != uuid.Nil {
			ids = append(ids, payload.ParentID)
		}
		if payload.ChildID != uuid.Nil {
			ids = append(ids, payload.ChildID)
		}
	}
	return ids
}
