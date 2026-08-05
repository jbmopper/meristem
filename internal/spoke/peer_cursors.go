package spoke

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
)

// CursorPurpose names what a bookmark is tracking. It is part of the stored key
// because a spoke keeps more than one kind of position against the same peer,
// and two positions that share a key silently overwrite each other. Feed
// observation and queue draining advance at different rates over different
// streams; collapsing them would make each one jump to the other's position,
// skipping whatever lay between.
type CursorPurpose string

const (
	// PurposeFeedObservation bookmarks a peer's event feed. It is pure
	// observability — nothing is re-projected from it.
	PurposeFeedObservation CursorPurpose = "feed"
	// PurposeQueueDrain bookmarks progress through a peer's command queue.
	PurposeQueueDrain CursorPurpose = "drain"
)

// PeerCursorKey builds the spoke_state key for one peer and purpose.
//
// It keys on the peer's node id, not its URL. A node's event sequence belongs
// to its identity, so an operator moving a peer to a new origin should not
// reset that peer's position and replay everything from the start. The legacy
// hub cursor is URL-keyed because node ids were not available where it was
// written; LegacyHubCursorKey preserves it exactly rather than migrating it,
// since rewriting the key on upgrade would look identical to having no cursor
// and replay the whole feed.
func PeerCursorKey(purpose CursorPurpose, peerNodeID string) (string, error) {
	switch purpose {
	case PurposeFeedObservation, PurposeQueueDrain:
	default:
		return "", fmt.Errorf("spoke: unknown cursor purpose %q", purpose)
	}
	if !domain.ValidNodeID(peerNodeID) {
		return "", fmt.Errorf("spoke: %q is not a DNS-safe node id", peerNodeID)
	}
	return "peer:" + string(purpose) + ":" + peerNodeID, nil
}

// LegacyHubCursorKey is the pre-mesh, URL-keyed feed bookmark. It is retained
// verbatim so an existing single-hub deployment keeps its position across the
// upgrade: a changed key is indistinguishable from an absent one, and an absent
// one replays the peer's whole feed.
func LegacyHubCursorKey(hubBaseURL string) string {
	return "hub_feed_cursor:" + hubBaseURL
}

// NewPeerCursorStore returns an event-backed cursor store scoped to one peer
// and purpose. Every peer advances independently: a peer that is unreachable
// for a week resumes from its own bookmark, not from wherever the reachable
// peers have since advanced to.
func NewPeerCursorStore(pool *pgxpool.Pool, writer *events.Writer, purpose CursorPurpose, peerNodeID string, actorID uuid.UUID, source domain.Source) (CursorStore, error) {
	key, err := PeerCursorKey(purpose, peerNodeID)
	if err != nil {
		return nil, err
	}
	return &pgCursorStore{
		pool:    pool,
		key:     key,
		service: NewCursorService(pool, writer),
		actorID: actorID,
		source:  source,
	}, nil
}
