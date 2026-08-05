package spoke

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/nodes"
)

// DefaultDrainGrace is how long a queue host keeps being drained after the
// operator removes it from this node's allowlist. It is generous on purpose:
// the cost of draining a peer that no longer holds anything is one empty poll,
// and the cost of stopping too early is a command that was already accepted
// sitting on that peer forever with nobody coming for it.
const DefaultDrainGrace = 24 * time.Hour

// DrainPeer is one queue host this node should poll for commands addressed to
// it.
type DrainPeer struct {
	// NodeID is the queue host's node id — the identity a credential is keyed
	// by, and what the drain loop reports failures against.
	NodeID string
	// DirectURL is the host's peer origin.
	DirectURL string
	// Retained is true when this host is no longer in the allowlist and is
	// being drained only to collect commands enqueued before it was removed.
	// The drain loop should not enqueue anything new here, and an operator
	// reading a status line should be able to tell the difference.
	Retained bool
}

// ResolveDrainPeers returns the queue hosts this node must poll, in the order
// it should poll them.
//
// The set is deliberately wider than the current allowlist. A command is
// accepted by whichever queue host was approved at enqueue time, and it stays
// there until its target drains it. If the operator then removes that host from
// queue_via, a drain loop that reads only the current allowlist stops polling
// the one peer that is actually holding work — the command is not lost, it is
// stranded, which is worse because nothing reports it.
//
// So the set is the current allowlist unioned with every host that appeared in
// this node's allowlist within the grace window. That history is recovered from
// the node's own route events rather than from a new table: the event log
// already records every allowlist this node has had, it survives restarts, and
// it needs no migration. An in-memory "recently removed" set would forget on
// the first restart, which is exactly when a stranded command matters most.
//
// Ordering is current-allowlist-first, in the operator's declared order, then
// retained hosts by node id. Deterministic order matters because it is what
// makes fairness testable.
//
// Hosts with no usable peer origin are omitted: there is nothing to poll. A
// host that is unreachable at request time is a different thing entirely and
// belongs to the drain loop's failure isolation, not here.
func ResolveDrainPeers(ctx context.Context, pool *pgxpool.Pool, localNodeID string, now time.Time, grace time.Duration) ([]DrainPeer, error) {
	if !domain.ValidNodeID(localNodeID) {
		return nil, fmt.Errorf("spoke: %q is not a DNS-safe node id", localNodeID)
	}
	if grace < 0 {
		return nil, fmt.Errorf("spoke: drain grace must not be negative, got %s", grace)
	}
	registry, err := nodes.List(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("spoke: read nodes projection: %w", err)
	}

	origins := make(map[string]string, len(registry))
	var current []string
	for _, node := range registry {
		if node.DirectURL != nil && *node.DirectURL != "" {
			origins[node.NodeID] = *node.DirectURL
		}
		if node.NodeID == localNodeID {
			current = node.RelayVia
		}
	}

	retained, err := recentlyRemovedQueueHosts(ctx, pool, localNodeID, now.Add(-grace))
	if err != nil {
		return nil, err
	}

	seen := make(map[string]bool, len(current)+len(retained))
	peers := make([]DrainPeer, 0, len(current)+len(retained))
	for _, host := range current {
		if seen[host] {
			continue
		}
		seen[host] = true
		if origin := origins[host]; origin != "" {
			peers = append(peers, DrainPeer{NodeID: host, DirectURL: origin})
		}
	}
	extra := make([]string, 0, len(retained))
	for host := range retained {
		if !seen[host] {
			extra = append(extra, host)
		}
	}
	sort.Strings(extra)
	for _, host := range extra {
		if origin := origins[host]; origin != "" {
			peers = append(peers, DrainPeer{NodeID: host, DirectURL: origin, Retained: true})
		}
	}
	return peers, nil
}

// recentlyRemovedQueueHosts reads every queue-host allowlist this node has
// declared since `since` out of its own route events, and returns their union.
//
// It reads the legacy relay_via key as well as queue_via because the rename is
// mid-flight on another branch and a node's history spans both spellings. A
// reader that understood only one of them would silently lose half the history
// and strand exactly the commands this function exists to find.
func recentlyRemovedQueueHosts(ctx context.Context, pool *pgxpool.Pool, localNodeID string, since time.Time) (map[string]bool, error) {
	rows, err := pool.Query(ctx, `
		SELECT payload
		FROM events
		WHERE subject_kind = $1 AND subject_id = $2 AND kind = ANY($3) AND occurred_at >= $4
		ORDER BY seq
	`, domain.SubjectNode, nodes.NodeSubjectID(localNodeID),
		[]string{domain.EventNodeRegistered, domain.EventNodeRouteUpdated}, since)
	if err != nil {
		return nil, fmt.Errorf("spoke: read node route history: %w", err)
	}
	defer rows.Close()

	hosts := make(map[string]bool)
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("spoke: scan node route history: %w", err)
		}
		var payload struct {
			QueueVia []string `json:"queue_via"`
			RelayVia []string `json:"relay_via"`
		}
		if err := json.Unmarshal(raw, &payload); err != nil {
			// A payload this reader cannot parse is history it cannot use, but
			// it is not grounds to abandon the whole lookup: the rest of the
			// history still names hosts that may be holding commands.
			continue
		}
		for _, host := range payload.QueueVia {
			hosts[host] = true
		}
		for _, host := range payload.RelayVia {
			hosts[host] = true
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("spoke: iterate node route history: %w", err)
	}
	return hosts, nil
}
