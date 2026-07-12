package crossnode

import (
	"time"

	"github.com/jbmopper/meristem/internal/domain"
)

// Select is the pure §2b route selection rule. Given the registry snapshot
// (nodes), the target node id, the sender's local cooldown map, and the
// current time, it returns the fixed-order candidate list a sender walks:
//
//  1. Direct — the target's own direct_url, if registered.
//  2. Durable queue — for each active node R in the target's relay_via IN
//     ORDER that has its own direct_url, a KindQueue candidate at R's
//     direct_url asks R to durably park the command for the target.
//
// Application-level forwarding is deliberately not emitted in the Stage 1
// contract. A direct route calls the target's canonical REST path; an
// inboundless node drains the durable queue. KindRelay remains a reserved wire
// value so old events/configuration can still be decoded, but it is not a
// selectable route until a complete target/credential/refusal contract exists.
//
// A candidate whose RouteKey is still cooling (present in cooldowns and within
// CooldownWindow of now) is skipped: a route the sender recently observed
// failing is local opinion, never shared state. cooldowns and nodes are read
// only; Select mutates neither.
//
// An unknown target returns ErrUnknownTarget. A known target with no surviving
// candidate (no direct_url, no reachable queue host, or everything cooling) returns
// ErrNoRoute. When exactly one node in the fleet registers a direct_url, this
// same rule collapses to direct-or-queue by itself: a direct-less target queues
// through that one reachable node.
func Select(nodes []domain.Node, target string, cooldowns map[string]time.Time, now time.Time) ([]Candidate, error) {
	byID := make(map[string]domain.Node, len(nodes))
	for _, n := range nodes {
		byID[n.NodeID] = n
	}
	targetNode, ok := byID[target]
	if !ok {
		return nil, ErrUnknownTarget
	}
	if targetNode.Status != domain.NodeStatusActive {
		return nil, ErrNoRoute
	}

	var candidates []Candidate
	add := func(c Candidate) {
		if cooling(cooldowns, c.RouteKey, now) {
			return
		}
		candidates = append(candidates, c)
	}

	// 1. Direct: the target's own registered inbound surface.
	if url := deref(targetNode.DirectURL); url != "" {
		add(Candidate{
			Kind:     KindDirect,
			URL:      url,
			NodeID:   target,
			RouteKey: routeKey(KindDirect, target, ""),
		})
	}

	// 2. Durable queue: each reachable queue host, in order, that can park the
	//    command for the (inbound-less) target to drain by outbound poll.
	for _, via := range targetNode.RelayVia {
		relay, ok := byID[via]
		if !ok || relay.Status != domain.NodeStatusActive {
			continue
		}
		if url := deref(relay.DirectURL); url != "" {
			add(Candidate{
				Kind:     KindQueue,
				URL:      url,
				NodeID:   via,
				Via:      via,
				RouteKey: routeKey(KindQueue, target, via),
			})
		}
	}

	if len(candidates) == 0 {
		return nil, ErrNoRoute
	}
	return candidates, nil
}

// cooling reports whether routeKey is in cooldowns and still inside the fixed
// CooldownWindow measured from the recorded failure time. A recorded time in
// the future (clock skew) is treated as still cooling.
func cooling(cooldowns map[string]time.Time, routeKey string, now time.Time) bool {
	failedAt, ok := cooldowns[routeKey]
	if !ok {
		return false
	}
	return now.Before(failedAt.Add(CooldownWindow))
}

// routeKey builds the stable cooldown identity for a route. The kind, target,
// and queue host together name the exact path so cooling a queue route does not
// cool the direct route.
func routeKey(kind CandidateKind, target, via string) string {
	if via == "" {
		return string(kind) + "|" + target
	}
	return string(kind) + "|" + target + "|" + via
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
