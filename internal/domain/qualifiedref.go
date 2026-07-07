package domain

import (
	"strings"

	"github.com/google/uuid"
)

// Cross-node reference reserve (docs/network-layer-spec.md §2 "Naming").
//
// A cross-node reference names an object by its home node and its local id in
// the qualified form `<node_id>:<uuid>`. An *unqualified* UUID always means
// "this node" — so references to local objects stay bare and round-trip
// unchanged, and only genuinely-remote references carry a prefix. Stage 0
// reserves the parse/format helpers and the DNS-safe node_id rule; nothing
// mints qualified refs yet.

// MaxNodeIDLen bounds a node_id at 32 characters so it stays a safe DNS label
// and a compact reference prefix.
const MaxNodeIDLen = 32

// ValidNodeID reports whether s is a well-formed, DNS-safe node_id:
// lowercase ASCII letters, digits, and internal hyphens; 1 to MaxNodeIDLen
// characters; no leading or trailing hyphen.
func ValidNodeID(s string) bool {
	if len(s) == 0 || len(s) > MaxNodeIDLen {
		return false
	}
	if s[0] == '-' || s[len(s)-1] == '-' {
		return false
	}
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= '0' && r <= '9':
		case r == '-':
		default:
			return false
		}
	}
	return true
}

// ParseQualifiedRef splits a cross-node reference into its home node_id and
// local UUID. A ref of the form `<node_id>:<uuid>` yields the node_id and the
// parsed UUID. A bare `<uuid>` (no prefix) means "this node": nodeID is empty
// and ok is true. ok is false when the node_id is not DNS-safe or the UUID
// does not parse.
//
// FormatQualifiedRef(ParseQualifiedRef(x)) reproduces x for every x this
// returns ok for — bare UUIDs round-trip to bare UUIDs.
func ParseQualifiedRef(ref string) (nodeID string, id uuid.UUID, ok bool) {
	if prefix, rest, found := strings.Cut(ref, ":"); found {
		parsed, err := uuid.Parse(rest)
		if err != nil || !ValidNodeID(prefix) {
			return "", uuid.Nil, false
		}
		return prefix, parsed, true
	}
	parsed, err := uuid.Parse(ref)
	if err != nil {
		return "", uuid.Nil, false
	}
	return "", parsed, true
}

// FormatQualifiedRef renders a cross-node reference. An empty nodeID means
// "this node" and produces the bare UUID; a non-empty nodeID produces the
// qualified `<node_id>:<uuid>` form. The nodeID is assumed valid
// (ValidNodeID); it is the caller's job to validate before formatting.
func FormatQualifiedRef(nodeID string, id uuid.UUID) string {
	if nodeID == "" {
		return id.String()
	}
	return nodeID + ":" + id.String()
}
