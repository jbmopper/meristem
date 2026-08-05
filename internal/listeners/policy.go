package listeners

// Base-policy normalization (docs/listener-control-plane.md, slice 2). A
// listener.policy_set payload is a COMPLETE replacement, never a patch, and
// only its persisted deterministic form controls delivery — prose proposals
// have no authority here. Predicates reuse the feed vocabulary verbatim so
// the policy fingerprint contract is the same pinned contract filtered feed
// cursors already rely on. Unknown predicate kinds, capabilities,
// projections, and focus modes fail closed.

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/feed"
)

var (
	ErrInvalidPolicy = errors.New("listeners: invalid policy")
)

// PolicyVersion is the only accepted payload_version for listener.policy_set.
const PolicyVersion = 1

// Focus modes: how the effective lens narrows while an assignment is held.
// claimed_work_item_tree is the design default; retain_base keeps the base
// lens alongside the assignment (a listener that may keep watching).
const (
	FocusClaimedWorkItemTree = "claimed_work_item_tree"
	FocusRetainBase          = "retain_base"
)

// PredicateWire is the transport shape of one policy predicate. Exactly one
// field group applies per kind, mirroring feed.Predicate's contract.
type PredicateWire struct {
	Kind       string   `json:"kind"`
	TokenID    string   `json:"token_id,omitempty"`
	TokenIDs   []string `json:"token_ids,omitempty"`
	WorkItemID string   `json:"work_item_id,omitempty"`
	EventKinds []string `json:"event_kinds,omitempty"`
}

// Policy is the normalized durable base policy. Empty Predicates means all
// eligible demand in the selected projection.
type Policy struct {
	PayloadVersion           int             `json:"payload_version"`
	ListenerID               uuid.UUID       `json:"listener_id"`
	Projection               string          `json:"projection,omitempty"`
	Predicates               []PredicateWire `json:"predicates"`
	Capabilities             []string        `json:"capabilities"`
	MaxConcurrentAssignments int             `json:"max_concurrent_assignments"`
	Focus                    string          `json:"focus"`
}

// feedPredicates maps the wire predicates onto the feed vocabulary,
// normalizing through the exact code filtered cursors use. The
// assigned_or_addressed lane is composed by the supervisor, never persisted
// in a base policy, so it is refused here.
func feedPredicates(wire []PredicateWire) ([]feed.Predicate, error) {
	out := make([]feed.Predicate, 0, len(wire))
	for _, w := range wire {
		kind := feed.PredicateKind(strings.TrimSpace(w.Kind))
		if kind == feed.PredicateAssignedOrAddressed {
			return nil, fmt.Errorf("%w: %s is composed by the supervisor, not persisted in a base policy", ErrInvalidPolicy, kind)
		}
		p := feed.Predicate{Kind: kind}
		if w.TokenID != "" {
			id, err := uuid.Parse(w.TokenID)
			if err != nil {
				return nil, fmt.Errorf("%w: predicate %s token_id: %v", ErrInvalidPolicy, kind, err)
			}
			p.TokenID = id
		}
		for _, raw := range w.TokenIDs {
			id, err := uuid.Parse(raw)
			if err != nil {
				return nil, fmt.Errorf("%w: predicate %s token_ids: %v", ErrInvalidPolicy, kind, err)
			}
			p.TokenIDs = append(p.TokenIDs, id)
		}
		if w.WorkItemID != "" {
			id, err := uuid.Parse(w.WorkItemID)
			if err != nil {
				return nil, fmt.Errorf("%w: predicate %s work_item_id: %v", ErrInvalidPolicy, kind, err)
			}
			p.WorkItemID = id
		}
		p.EventKinds = w.EventKinds
		out = append(out, p)
	}
	return out, nil
}

func wirePredicates(preds []feed.Predicate) []PredicateWire {
	out := make([]PredicateWire, 0, len(preds))
	for _, p := range preds {
		w := PredicateWire{Kind: string(p.Kind)}
		if p.TokenID != uuid.Nil {
			w.TokenID = p.TokenID.String()
		}
		for _, id := range p.TokenIDs {
			w.TokenIDs = append(w.TokenIDs, id.String())
		}
		if p.WorkItemID != uuid.Nil {
			w.WorkItemID = p.WorkItemID.String()
		}
		w.EventKinds = p.EventKinds
		out = append(out, w)
	}
	return out
}

// NormalizePolicy validates and canonicalizes a full-replacement policy for
// listenerID against the listener's registered capabilities. It returns the
// normalized policy plus the fingerprint of its predicate set — the same
// 128-bit contract feed cursors pin.
func NormalizePolicy(p Policy, listenerID uuid.UUID, registeredCapabilities []string) (Policy, string, error) {
	if p.PayloadVersion != 0 && p.PayloadVersion != PolicyVersion {
		return Policy{}, "", fmt.Errorf("%w: unsupported payload_version %d", ErrInvalidPolicy, p.PayloadVersion)
	}
	if p.ListenerID != uuid.Nil && p.ListenerID != listenerID {
		return Policy{}, "", fmt.Errorf("%w: policy listener_id %s does not name listener %s", ErrInvalidPolicy, p.ListenerID, listenerID)
	}
	focus := strings.TrimSpace(p.Focus)
	if focus == "" {
		focus = FocusClaimedWorkItemTree
	}
	if focus != FocusClaimedWorkItemTree && focus != FocusRetainBase {
		return Policy{}, "", fmt.Errorf("%w: unknown focus mode %q", ErrInvalidPolicy, p.Focus)
	}
	if p.MaxConcurrentAssignments == 0 {
		p.MaxConcurrentAssignments = 1
	}
	if p.MaxConcurrentAssignments != 1 {
		return Policy{}, "", fmt.Errorf("%w: max_concurrent_assignments must be exactly 1 in this release", ErrInvalidPolicy)
	}
	capabilities, err := normalizeCapabilities(p.Capabilities)
	if err != nil {
		return Policy{}, "", err
	}
	for _, capability := range capabilities {
		if !slices.Contains(registeredCapabilities, capability) {
			return Policy{}, "", fmt.Errorf("%w: capability %q is not registered for this listener", ErrInvalidPolicy, capability)
		}
	}
	if len(capabilities) == 0 {
		// A policy without an explicit capability subset offers everything
		// the registration offers.
		capabilities = append([]string(nil), registeredCapabilities...)
	}
	preds, err := feedPredicates(p.Predicates)
	if err != nil {
		return Policy{}, "", err
	}
	normalized, err := feed.NormalizeReadFilter(feed.ReadFilter{Predicates: preds})
	if err != nil {
		return Policy{}, "", fmt.Errorf("%w: %v", ErrInvalidPolicy, err)
	}
	out := Policy{
		PayloadVersion:           PolicyVersion,
		ListenerID:               listenerID,
		Projection:               strings.TrimSpace(p.Projection),
		Predicates:               wirePredicates(normalized.Predicates),
		Capabilities:             capabilities,
		MaxConcurrentAssignments: 1,
		Focus:                    focus,
	}
	return out, normalized.FingerprintHash(), nil
}

// Narrows reports whether next only narrows prior: predicates AND together,
// so a superset of predicates narrows; capabilities may only shrink; the
// projection and concurrency may not change. This is the deterministic rule
// that lets a listener's own principal replace its policy without the admin
// scope — anything else requires listener administration authority.
func Narrows(prior, next Policy) bool {
	if prior.Projection != next.Projection {
		return false
	}
	if next.MaxConcurrentAssignments > prior.MaxConcurrentAssignments {
		return false
	}
	nextKeys := predicateKeySet(next.Predicates)
	for key := range predicateKeySet(prior.Predicates) {
		if !nextKeys[key] {
			return false
		}
	}
	priorCaps := make(map[string]bool, len(prior.Capabilities))
	for _, c := range prior.Capabilities {
		priorCaps[c] = true
	}
	for _, c := range next.Capabilities {
		if !priorCaps[c] {
			return false
		}
	}
	return true
}

func predicateKeySet(wire []PredicateWire) map[string]bool {
	out := make(map[string]bool, len(wire))
	for _, w := range wire {
		key := fmt.Sprintf("%s|%s|%v|%s|%v", w.Kind, w.TokenID, w.TokenIDs, w.WorkItemID, w.EventKinds)
		out[key] = true
	}
	return out
}

func normalizeCapabilities(raw []string) ([]string, error) {
	seen := make(map[string]bool, len(raw))
	out := make([]string, 0, len(raw))
	for _, capability := range raw {
		trimmed := strings.TrimSpace(capability)
		if trimmed == "" {
			return nil, fmt.Errorf("%w: empty capability name", ErrInvalidPolicy)
		}
		if !seen[trimmed] {
			seen[trimmed] = true
			out = append(out, trimmed)
		}
	}
	slices.Sort(out)
	return out, nil
}
