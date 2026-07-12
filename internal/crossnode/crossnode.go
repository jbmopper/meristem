// Package crossnode carries a command from the node that issues it to the
// home node that owns the target object, following the deterministic route
// selection of docs/network-layer-spec.md §2b ("Route table and selection:
// making the mesh boring").
//
// The mesh has no routing protocol: topology is registry data (the `nodes`
// projection) and route choice is a pure function of a registry snapshot plus
// a local, non-gossiped cooldown list. This package owns three concerns:
//
//   - Select: the pure §2b selection rule — given the registry snapshot, a
//     target node id, and the sender's local cooldowns, produce the fixed
//     ordered candidate list (direct, then durable queue fallback).
//   - Deliver: the client-side walk of that candidate list over HTTP,
//     advancing on transport failure / 5xx (recording a cooldown) and stopping
//     on a definitive 2xx or 4xx.
//   - the server-side command_queue projection (queue.go, projectors.go): when
//     a reachable node is asked to durably park a command for an inbound-less
//     target, it appends command.queued and folds it into command_queue.
//
// Selection is deliberately boring: first success wins, application-level
// relay is deferred, and the whole scheme collapses to direct-or-queue when
// exactly one node registers a direct_url — the guaranteed fallback the fleet
// is built on.
package crossnode

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/domain"
)

// CooldownWindow is how long a route the sender observed failing is skipped by
// that sender before it is retried. §2b fixes this at 60s and calls it local
// opinion — never shared state to reconcile.
const CooldownWindow = 60 * time.Second

// CandidateKind names direct and queue delivery plus the reserved historical
// relay value. Select emits only direct and queue in Stage 1.
type CandidateKind string

const (
	// KindDirect posts straight to the target's own registered direct_url.
	KindDirect CandidateKind = "direct"
	// KindRelay is reserved for historical/prototype configuration. Select does
	// not emit it until forwarding has a complete target/credential contract.
	KindRelay CandidateKind = "relay"
	// KindQueue posts to a reachable queue host's direct_url asking it to
	// durably park the command (command.queued) for an inbound-less target,
	// which drains it by outbound poll.
	KindQueue CandidateKind = "queue"
)

// Candidate is one delivery attempt Select produced: where to POST, by which
// mechanism, and the stable RouteKey the sender uses to cool the route down on
// failure. The zero Candidate is not meaningful; Select always sets Kind, URL,
// and RouteKey.
type Candidate struct {
	// Kind selects the headers Deliver attaches and how a failure is judged.
	Kind CandidateKind
	// URL is the origin to POST to: the target's direct_url for KindDirect or
	// the queue host's direct_url for KindQueue.
	URL string
	// NodeID identifies the node that terminates this HTTP attempt and therefore
	// the node whose bearer credential must be used. For a direct candidate it
	// is the target node; for a durable queue candidate it is the reachable
	// queue host in Via. Credential material never lives on Candidate itself.
	NodeID string
	// Via is the queue host node id for KindQueue (or reserved relay node for
	// KindRelay); empty for KindDirect. The queue header uses the target node
	// id, not Via.
	Via string
	// RouteKey uniquely and stably identifies this route across a Select /
	// Deliver / cooldown cycle. A failed route is keyed by RouteKey in the
	// sender's local cooldown map; Select skips a candidate whose RouteKey is
	// still cooling.
	RouteKey string
}

// Command is the cross-node envelope Deliver posts and the queue projector
// records: the home-node REST call to replay, plus the attribution that must
// cross the boundary with it.
type Command struct {
	// OriginNodeID is the DNS-safe node issuing the command. It is transported
	// only as structural provenance; the receiver still derives actor identity
	// and source exclusively from its locally authenticated bearer.
	OriginNodeID string
	// OriginActorTokenID and OriginActorSource identify the actor on the
	// originating node. They are provenance only: the receiving event remains
	// attributed to the target-local bearer that authorized execution.
	OriginActorTokenID *uuid.UUID
	OriginActorSource  domain.Source
	// CausingWorkItemID is the origin-homed item whose finite delivery policy
	// owns this command. Nil means the caller has no owning work item.
	CausingWorkItemID *uuid.UUID
	// TargetNodeID is the DNS-safe home node id the command is bound for. It
	// is the X-Meristem-Queue-For value for KindQueue candidates.
	TargetNodeID string
	// IdempotencyKey is the caller-supplied Idempotency-Key. It rides as the
	// HTTP header on every attempt (so retries collapse at the home node's
	// idempotency middleware) and is recorded as origin_idempotency_key on a
	// queued command.
	IdempotencyKey string
	// Path is the home-node REST path the command executes, e.g.
	// "/v1/work-items/<id>/transition". Recorded as command_path when queued.
	Path string
	// Body is the JSON request body of that home-node call. Recorded verbatim
	// as command_body when queued.
	Body json.RawMessage
}

// BearerResolver returns the target-node credential for one HTTP attempt.
// Tokens are node-local, so a direct attempt and a queue fallback can require
// different bearers. Implementations must not log or persist the returned
// value.
type BearerResolver func(ctx context.Context, nodeID string) (string, error)

// Attempt records the outcome of one candidate Deliver tried, for tests and
// observability. Err is set for a transport failure; StatusCode is set for an
// HTTP response.
type Attempt struct {
	Candidate  Candidate
	StatusCode int
	Err        error
	// CooledDown is true when this attempt caused RouteKey to be cooled down
	// (transport failure or 5xx).
	CooledDown bool
}

// Outcome is the result of a Deliver walk. Exactly one of Delivered (a 2xx) or
// a definitive 4xx (Terminal4xx) ends the walk; if every candidate failed at
// the transport/5xx level the walk exhausts and Deliver returns
// ErrAllRoutesFailed with Delivered false. Cooldowns is the updated cooldown
// map (the input map is never mutated).
type Outcome struct {
	// Delivered is true iff a candidate returned a 2xx.
	Delivered bool
	// Terminal is the candidate that produced the terminal response (2xx or
	// 4xx). Zero when the walk exhausted with no HTTP response at all.
	Terminal Candidate
	// StatusCode is the terminal HTTP status (0 when the walk exhausted).
	StatusCode int
	// Body is the terminal response body (nil when the walk exhausted).
	Body []byte
	// Cooldowns is the input cooldown map plus any routes this walk cooled.
	Cooldowns map[string]time.Time
	// Attempts is the per-candidate trace, in the order tried.
	Attempts []Attempt
}

var (
	// ErrUnknownTarget is returned by Select when target is not a node in the
	// registry snapshot.
	ErrUnknownTarget = errors.New("crossnode: unknown target node")
	// ErrNoRoute is returned by Select when the target is known but no
	// candidate route survives (no direct_url, no reachable relay, all routes
	// cooling down).
	ErrNoRoute = errors.New("crossnode: no route to target")
	// ErrAllRoutesFailed is returned by Deliver when every candidate failed at
	// the transport or 5xx level and the walk exhausted without a definitive
	// 2xx or 4xx.
	ErrAllRoutesFailed = errors.New("crossnode: all candidate routes failed")
	// ErrMissingCredential is returned before an attempt when no node-specific
	// bearer is available. Falling through to unauthenticated HTTP would destroy
	// attribution and can never be a valid route attempt.
	ErrMissingCredential = errors.New("crossnode: missing node credential")
	// ErrUnsupportedRoute marks the deferred application-level relay route.
	// Stage 1 supports direct canonical REST plus durable queue fallback only.
	ErrUnsupportedRoute = errors.New("crossnode: application relay is not supported")
	// ErrInvalidCommandPath protects the pull-only executor from becoming an
	// arbitrary authenticated POST proxy.
	ErrInvalidCommandPath = errors.New("crossnode: command path is not allowed")
	// ErrInvalidOrigin marks malformed or unsafe registered route origins.
	ErrInvalidOrigin = errors.New("crossnode: route origin is invalid")
)

// ValidateCommandPath permits only the narrow work-item mutation surface used
// to progress a remotely-homed item. Approval decisions, connectors, token
// administration, inbox capture, and arbitrary REST paths are deliberately
// excluded from queued execution.
func ValidateCommandPath(raw string) error {
	if strings.TrimSpace(raw) != raw || raw == "" {
		return ErrInvalidCommandPath
	}
	u, err := url.ParseRequestURI(raw)
	if err != nil || u.IsAbs() || u.Host != "" || u.RawQuery != "" || u.Fragment != "" {
		return ErrInvalidCommandPath
	}
	if path.Clean(u.Path) != u.Path || strings.Contains(u.Path, "//") {
		return ErrInvalidCommandPath
	}
	if u.Path == "/v1/work-items" {
		return nil
	}
	parts := strings.Split(strings.TrimPrefix(u.Path, "/"), "/")
	if len(parts) != 4 || parts[0] != "v1" || parts[1] != "work-items" {
		return ErrInvalidCommandPath
	}
	if _, err := uuid.Parse(parts[2]); err != nil {
		return ErrInvalidCommandPath
	}
	switch parts[3] {
	case "children", "events", "metadata", "transition", "convergence-proposal":
		return nil
	default:
		return ErrInvalidCommandPath
	}
}

// ValidateOrigin accepts an origin-only HTTPS URL, plus plaintext loopback for
// local development. Paths, userinfo, queries, and fragments are forbidden so
// Deliver can append a canonical REST path without ambiguity or credential
// leakage. Private/LAN HTTP requires TLS termination or an explicit future
// policy; it is not silently trusted here.
func ValidateOrigin(raw string) error {
	if err := domain.ValidateNodeOrigin(raw); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidOrigin, err)
	}
	return nil
}
