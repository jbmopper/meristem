package feed

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	// Directed signals to the READER outrank authorship quieting: an event the
	// excluded actor authored survives only when the composed assigned lane
	// proved it is addressed to that lane's reader and the reader is not the
	// excluded token itself. Everything else the excluded actor authored is
	// removed — including its outbound events addressed to third parties, so
	// exclude_actor=self can never echo the caller's own directed writes.
	PredicateExcludeActor PredicateKind = "exclude_actor"

	// PredicateActor keeps only events authored by TokenID — the inclusion
	// counterpart of exclude_actor, for watching one agent's activity. Like
	// kind predicates it is a content filter: items the assigned lane matched
	// as addressed survive it, so lensing to one author cannot swallow a
	// system-authored release or handback directed at the reader.
	PredicateActor PredicateKind = "actor"

	// PredicateWorkItem and PredicateWorkItemTree keep only events anchored
	// (via WorkItemAnchors) to WorkItemID exactly, or to any item in the
	// subtree rooted at WorkItemID. Anchor scoping is deliberate spatial
	// narrowing, so directed-signal protection does NOT apply: a watcher
	// lensed to one tree accepts missing wakes from elsewhere — those still
	// arrive on its unscoped assigned lane.
	PredicateWorkItem     PredicateKind = "work_item"
	PredicateWorkItemTree PredicateKind = "work_item_tree"

	// PredicateKindInclude and PredicateKindExclude narrow by event kind
	// without defining a named projection. Kind predicates are content
	// filters: when composed with the assigned lane they never remove an
	// event the lane matched as ADDRESSED (explicit addressee, assignment
	// control, or terminal handback), and query-level kind pushdown retains
	// those kinds, so a kind-lensed listener cannot silently lose its wake
	// signals at either level.
	PredicateKindInclude PredicateKind = "kind_include"
	PredicateKindExclude PredicateKind = "kind_exclude"
)

var (
	ErrUnknownPredicate = errors.New("feed: unknown predicate")
	ErrInvalidPredicate = errors.New("feed: invalid predicate")
)

// Predicate is one reducer in a ReadFilter. Predicates are ANDed; any OR
// relationship belongs inside a named predicate's definition. TokenID is
// explicit identity, never a name or a value inferred from prose. Each
// PredicateKind uses exactly one field group — token identity, work-item
// identity, or event kinds — and normalization rejects any other shape.
type Predicate struct {
	Kind       PredicateKind
	TokenID    uuid.UUID
	WorkItemID uuid.UUID
	EventKinds []string
}

// canonicalKey is the normalized predicate's identity: dedupe, deterministic
// ordering, and the filter fingerprint all derive from it. EventKinds must be
// canonicalized (trimmed, sorted, deduped) before this is meaningful.
func (p Predicate) canonicalKey() string {
	// JSON encoding is unambiguous under arbitrary kind strings — delimiter
	// characters inside EventKinds entries cannot collide across sets.
	kinds := p.EventKinds
	if len(kinds) == 0 {
		// nil and empty are one identity; encode both as [].
		kinds = []string{}
	}
	raw, err := json.Marshal([]any{string(p.Kind), p.TokenID.String(), p.WorkItemID.String(), kinds})
	if err != nil {
		// Marshaling strings cannot fail; keep the contract total anyway.
		return string(p.Kind) + "|" + p.TokenID.String() + "|" + p.WorkItemID.String()
	}
	return string(raw)
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
		normalized, err := normalizePredicate(predicate)
		if err != nil {
			return ReadFilter{}, err
		}
		key := normalized.canonicalKey()
		if seen[key] {
			continue
		}
		seen[key] = true
		out.Predicates = append(out.Predicates, normalized)
	}
	slices.SortFunc(out.Predicates, func(a, b Predicate) int {
		return strings.Compare(a.canonicalKey(), b.canonicalKey())
	})
	return out, nil
}

func normalizePredicate(p Predicate) (Predicate, error) {
	switch p.Kind {
	case PredicateAssignedOrAddressed, PredicateActor, PredicateExcludeActor:
		if p.TokenID == uuid.Nil {
			return Predicate{}, fmt.Errorf("%w: %s requires token identity", ErrInvalidPredicate, p.Kind)
		}
		if p.WorkItemID != uuid.Nil || len(p.EventKinds) != 0 {
			return Predicate{}, fmt.Errorf("%w: %s accepts only token identity", ErrInvalidPredicate, p.Kind)
		}
		// A non-nil empty slice is semantically identical to nil; collapse it
		// so canonical identity and dedupe cannot split on representation.
		p.EventKinds = nil
	case PredicateWorkItem, PredicateWorkItemTree:
		if p.WorkItemID == uuid.Nil {
			return Predicate{}, fmt.Errorf("%w: %s requires work item identity", ErrInvalidPredicate, p.Kind)
		}
		if p.TokenID != uuid.Nil || len(p.EventKinds) != 0 {
			return Predicate{}, fmt.Errorf("%w: %s accepts only work item identity", ErrInvalidPredicate, p.Kind)
		}
		p.EventKinds = nil
	case PredicateKindInclude, PredicateKindExclude:
		if p.TokenID != uuid.Nil || p.WorkItemID != uuid.Nil {
			return Predicate{}, fmt.Errorf("%w: %s accepts only event kinds", ErrInvalidPredicate, p.Kind)
		}
		kinds := make([]string, 0, len(p.EventKinds))
		for _, kind := range p.EventKinds {
			kind = strings.TrimSpace(kind)
			if kind == "" {
				return Predicate{}, fmt.Errorf("%w: %s contains an empty event kind", ErrInvalidPredicate, p.Kind)
			}
			if !knownEventKind(kind) {
				return Predicate{}, fmt.Errorf("%w: %s names unknown event kind %q", ErrInvalidPredicate, p.Kind, kind)
			}
			kinds = append(kinds, kind)
		}
		if len(kinds) == 0 {
			return Predicate{}, fmt.Errorf("%w: %s requires event kinds", ErrInvalidPredicate, p.Kind)
		}
		slices.Sort(kinds)
		p.EventKinds = slices.Compact(kinds)
	default:
		return Predicate{}, fmt.Errorf("%w: %s", ErrUnknownPredicate, p.Kind)
	}
	return p, nil
}

// knownEventKind reports whether kind is in the feed-visible catalog: the
// default included kinds plus the assigned-lane runtime control kinds.
// Unknown or misspelled kinds fail normalization closed rather than silently
// matching nothing.
func knownEventKind(kind string) bool {
	return slices.Contains(IncludedKinds, kind) ||
		kind == domain.EventWorkItemAssigned ||
		kind == domain.EventWorkItemAssignmentReleased
}

// FingerprintHash is the compact channel-identity form of the canonical
// predicate key: empty when no predicates apply (plain feed), else the first
// 128 bits of SHA-256(CanonicalPredicateKey). 128 bits matches the
// deterministic event-id collision margin: with UUID-valued predicates a
// birthday collision needs ~2^64 candidate filters, so equality of the
// fingerprint is safe to treat as filter identity. Same stability contract
// as the key; the width is pinned by contract test.
func (f ReadFilter) FingerprintHash() string {
	if len(f.Predicates) == 0 {
		return ""
	}
	sum := sha256.Sum256([]byte(f.CanonicalPredicateKey()))
	return hex.EncodeToString(sum[:16])
}

// CanonicalPredicateKey is the deterministic encoding of the normalized
// predicate set: identical logical filters yield identical keys regardless of
// input order or duplication. Named Channels hashes this (with projection
// name/version) into cursor identity, so the encoding is a compatibility
// contract — existing predicate kinds must keep their encoding stable, and
// the vocabulary grows append-only.
func (f ReadFilter) CanonicalPredicateKey() string {
	keys := make([]string, 0, len(f.Predicates))
	for _, predicate := range f.Predicates {
		keys = append(keys, predicate.canonicalKey())
	}
	return strings.Join(keys, ";")
}

func (f ReadFilter) queryKinds() []string {
	assigned := false
	for _, predicate := range f.Predicates {
		if predicate.Kind == PredicateAssignedOrAddressed {
			assigned = true
			break
		}
	}
	var kinds []string
	if f.Projection == nil {
		kinds = slices.Clone(IncludedKinds)
	} else {
		kinds = f.Projection.QueryKinds()
	}
	// Kind pushdown is an optimization, never the authority: matchingItems
	// re-evaluates the same predicates per item. Under the assigned lane it
	// is disabled entirely — ANY base kind can carry an explicit addressee,
	// so dropping kinds at the SQL layer would lose addressed wake signals
	// before the protection pass can see them.
	if !assigned {
		for _, predicate := range f.Predicates {
			switch predicate.Kind {
			case PredicateKindInclude:
				kinds = filterQueryKinds(kinds, func(kind string) bool {
					return slices.Contains(predicate.EventKinds, kind)
				})
			case PredicateKindExclude:
				kinds = filterQueryKinds(kinds, func(kind string) bool {
					return !slices.Contains(predicate.EventKinds, kind)
				})
			}
		}
	}
	if assigned && f.Projection == nil {
		// Assignment lifecycle remains audit-only/admin for the default
		// feed and persisted projections. The actor-addressed runtime lane
		// is the narrow exception that wakes an idle claimant on assign or
		// release without reclassifying either event kind.
		kinds = append(kinds, domain.EventWorkItemAssigned, domain.EventWorkItemAssignmentReleased)
	}
	return kinds
}

func filterQueryKinds(kinds []string, keep func(string) bool) []string {
	out := make([]string, 0, len(kinds))
	for _, kind := range kinds {
		if keep(kind) {
			out = append(out, kind)
		}
	}
	return out
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
	// protectedBy records, per item, the reader token of an assigned lane that
	// matched the item as ADDRESSED (explicit addressee, assignment control,
	// terminal handback). Content predicates (actor, kind_include,
	// kind_exclude) skip protected items and exclude_actor compares the
	// reader identity, so a lensed listener keeps its wake signals.
	// Evaluation runs in two structural phases — assigned_or_addressed
	// first, everything else after — so protection is computed before it is
	// consulted regardless of canonical predicate ordering.
	protectedBy := make([]uuid.UUID, len(items))
	ordered := make([]Predicate, 0, len(filter.Predicates))
	for _, predicate := range filter.Predicates {
		if predicate.Kind == PredicateAssignedOrAddressed {
			ordered = append(ordered, predicate)
		}
	}
	for _, predicate := range filter.Predicates {
		if predicate.Kind != PredicateAssignedOrAddressed {
			ordered = append(ordered, predicate)
		}
	}
	for _, predicate := range ordered {
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
					if addressed {
						protectedBy[i] = predicate.TokenID
					}
					continue
				}
				if addressed {
					protectedBy[i] = predicate.TokenID
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
			// sole carve-out is lane-proven: the item survives only when an
			// assigned lane marked it addressed to a reader other than the
			// excluded token. Without a composed assigned lane there is no
			// reader identity, so exclusion is literal.
			for i, item := range items {
				if !matches[i] || item.ActorTokenID == nil || *item.ActorTokenID != predicate.TokenID {
					continue
				}
				if protectedBy[i] != uuid.Nil && protectedBy[i] != predicate.TokenID {
					continue
				}
				matches[i] = false
			}
		case PredicateActor:
			for i, item := range items {
				if matches[i] && protectedBy[i] == uuid.Nil && (item.ActorTokenID == nil || *item.ActorTokenID != predicate.TokenID) {
					matches[i] = false
				}
			}
		case PredicateWorkItem:
			for i, item := range items {
				if matches[i] && !slices.Contains(WorkItemAnchors(item), predicate.WorkItemID) {
					matches[i] = false
				}
			}
		case PredicateWorkItemTree:
			ids := candidateAnchorIDs(items, matches, nil)
			inTree, err := s.anchorsInSubtree(ctx, predicate.WorkItemID, ids)
			if err != nil {
				return nil, err
			}
			for i, item := range items {
				if !matches[i] {
					continue
				}
				matches[i] = false
				for _, id := range WorkItemAnchors(item) {
					if inTree[id] {
						matches[i] = true
						break
					}
				}
			}
		case PredicateKindInclude:
			for i, item := range items {
				if matches[i] && protectedBy[i] == uuid.Nil && !slices.Contains(predicate.EventKinds, item.Kind) {
					matches[i] = false
				}
			}
		case PredicateKindExclude:
			for i, item := range items {
				if matches[i] && protectedBy[i] == uuid.Nil && slices.Contains(predicate.EventKinds, item.Kind) {
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

// anchorsInSubtree resolves which candidate work-item ids sit inside the
// subtree rooted at root, in one recursive walk (the same shape the access
// package uses for tree-scoped visibility).
func (s *Service) anchorsInSubtree(ctx context.Context, root uuid.UUID, ids []uuid.UUID) (map[uuid.UUID]bool, error) {
	matched := make(map[uuid.UUID]bool, len(ids))
	if len(ids) == 0 {
		return matched, nil
	}
	rows, err := s.pool.Query(ctx, `
		WITH RECURSIVE subtree(id) AS (
			SELECT $1::uuid
			UNION
			SELECT wir.child_id
			FROM work_item_relations wir
			JOIN subtree s ON wir.parent_id = s.id
		)
		SELECT DISTINCT id FROM subtree WHERE id = ANY($2::uuid[])
	`, root, ids)
	if err != nil {
		return nil, fmt.Errorf("feed: resolve work item subtree: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("feed: scan work item subtree: %w", err)
		}
		matched[id] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("feed: resolve work item subtree: %w", err)
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
