package spoke

import (
	"testing"
)

// TestPeerCursorKeysAreIndependentPerPeer is convergence check 4's first half.
// Two peers sharing a key would each jump to the other's position on every
// tick, skipping whatever lay between — and the skip is silent, because a
// cursor that moved forward looks exactly like progress.
func TestPeerCursorKeysAreIndependentPerPeer(t *testing.T) {
	hub, err := PeerCursorKey(PurposeFeedObservation, "hub")
	if err != nil {
		t.Fatalf("hub key: %v", err)
	}
	den, err := PeerCursorKey(PurposeFeedObservation, "den")
	if err != nil {
		t.Fatalf("den key: %v", err)
	}
	if hub == den {
		t.Fatalf("two peers share cursor key %q", hub)
	}
}

// TestDrainAndFeedCursorsNeverCollide is the second half: the same peer's
// drain and feed positions track different streams advancing at different
// rates. One key for both means each purpose keeps overwriting the other.
func TestDrainAndFeedCursorsNeverCollide(t *testing.T) {
	feed, err := PeerCursorKey(PurposeFeedObservation, "hub")
	if err != nil {
		t.Fatalf("feed key: %v", err)
	}
	drain, err := PeerCursorKey(PurposeQueueDrain, "hub")
	if err != nil {
		t.Fatalf("drain key: %v", err)
	}
	if feed == drain {
		t.Fatalf("feed and drain share cursor key %q for the same peer", feed)
	}
}

// TestPeerCursorKeyDoesNotCollideWithTheLegacyHubKey guards the upgrade. If a
// new peer key could equal the old URL-keyed one, an upgraded deployment would
// read a feed position as a drain position or vice versa.
func TestPeerCursorKeyDoesNotCollideWithTheLegacyHubKey(t *testing.T) {
	legacy := LegacyHubCursorKey("https://hub.example")
	for _, purpose := range []CursorPurpose{PurposeFeedObservation, PurposeQueueDrain} {
		for _, peer := range []string{"hub", "den", "m4"} {
			key, err := PeerCursorKey(purpose, peer)
			if err != nil {
				t.Fatalf("key(%s,%s): %v", purpose, peer, err)
			}
			if key == legacy {
				t.Fatalf("peer key %q collides with the legacy hub key", key)
			}
		}
	}
}

// TestLegacyHubCursorKeyIsUnchanged pins the exact stored string. Changing it
// is indistinguishable from having no cursor at all, and an absent cursor
// replays the peer's entire feed on the first tick after an upgrade.
func TestLegacyHubCursorKeyIsUnchanged(t *testing.T) {
	if got, want := LegacyHubCursorKey("https://hub.example"), "hub_feed_cursor:https://hub.example"; got != want {
		t.Fatalf("legacy key = %q, want %q — changing it silently replays the whole feed", got, want)
	}
}

// TestPeerCursorKeyIsKeyedOnIdentityNotOrigin states the deliberate difference
// from the legacy scheme: moving a peer to a new URL must not reset its
// position, because the sequence belongs to the node, not the address.
func TestPeerCursorKeyIsKeyedOnIdentityNotOrigin(t *testing.T) {
	before, err := PeerCursorKey(PurposeFeedObservation, "den")
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	after, err := PeerCursorKey(PurposeFeedObservation, "den")
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	if before != after {
		t.Fatal("the same peer produced two different cursor keys")
	}
	if want := "peer:feed:den"; before != want {
		t.Fatalf("key = %q, want %q (no origin component)", before, want)
	}
}

// TestPeerCursorKeyFailsClosed keeps a malformed peer or purpose from minting a
// key. A key built from an unvalidated id is how one peer's bookmark ends up
// addressable by another.
func TestPeerCursorKeyFailsClosed(t *testing.T) {
	for _, peer := range []string{"", "HUB", "hub.example", "-hub", "hub-", "hub:8080", "a/b"} {
		if key, err := PeerCursorKey(PurposeFeedObservation, peer); err == nil {
			t.Errorf("PeerCursorKey(feed, %q) = %q, want refusal", peer, key)
		}
	}
	for _, purpose := range []CursorPurpose{"", "queue", "drain ", "FEED"} {
		if key, err := PeerCursorKey(purpose, "hub"); err == nil {
			t.Errorf("PeerCursorKey(%q, hub) = %q, want refusal", purpose, key)
		}
	}
}
