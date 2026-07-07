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
//  2. Relay — for each node R in the target's relay_via IN ORDER that has its
//     own direct_url, a KindRelay candidate at R's direct_url.
//  3. Queue — for each such reachable relay R (same order), a KindQueue
//     candidate at R's direct_url that asks R to durably park the command for
//     the target.
//
// A candidate whose RouteKey is still cooling (present in cooldowns and within
// CooldownWindow of now) is skipped: a route the sender recently observed
// failing is local opinion, never shared state. cooldowns and nodes are read
// only; Select mutates neither.
//
// An unknown target returns ErrUnknownTarget. A known target with no surviving
// candidate (no direct_url, no reachable relay, or everything cooling) returns
// ErrNoRoute. When exactly one node in the fleet registers a direct_url, this
// same rule collapses to hub-and-spoke by itself: every delivery to a
// direct-less target becomes relay-or-queue through that one node.
func Select(nodes []domain.Node, target string, cooldowns map[string]time.Time, now time.Time) ([]Candidate, error) {
	byID := make(map[string]domain.Node, len(nodes))
	for _, n := range nodes {
		byID[n.NodeID] = n
	}
	targetNode, ok := byID[target]
	if !ok {
		return nil, ErrUnknownTarget
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
			RouteKey: routeKey(KindDirect, target, ""),
		})
	}

	// 2. Relay hops: each relay_via node, in order, that has a direct_url of
	//    its own to forward through.
	for _, via := range targetNode.RelayVia {
		relay, ok := byID[via]
		if !ok {
			continue
		}
		if url := deref(relay.DirectURL); url != "" {
			add(Candidate{
				Kind:     KindRelay,
				URL:      url,
				Via:      via,
				RouteKey: routeKey(KindRelay, target, via),
			})
		}
	}

	// 3. Durable queue: each reachable relay node, in order, that can park the
	//    command for the (inbound-less) target to drain by outbound poll.
	for _, via := range targetNode.RelayVia {
		relay, ok := byID[via]
		if !ok {
			continue
		}
		if url := deref(relay.DirectURL); url != "" {
			add(Candidate{
				Kind:     KindQueue,
				URL:      url,
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
// and relay hop together name the exact path so cooling a relay hop does not
// cool the direct route (or the queue variant) through the same node.
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
