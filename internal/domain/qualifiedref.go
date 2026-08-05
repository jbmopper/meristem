package domain

import (
	"net/url"
	"strings"

	"github.com/google/uuid"
)

// Cross-node reference reserve (docs/network-layer-spec.md §2 "Naming").
//
// A cross-node reference names an object by its home node and its local id.
// There are three accepted spellings, and they all normalize to the same
// (node_id, uuid) tuple:
//
//   - `<uuid>` — unqualified, and therefore always local to the interpreting
//     node. A bare UUID is never remote-discovered.
//   - `<node_id>:<uuid>` — the compact qualified alias, used in-process and on
//     internal call paths where a full URI is noise.
//   - `mrs://<node_id>/work-items/<uuid>` — the canonical form. This is the
//     one that goes out on external and durable surfaces, per docs/spec.md
//     §"Naming" and docs/network-layer-spec.md §2.
//
// The split matters because the two qualified spellings have different jobs.
// The compact alias is a convenience; the canonical URI is the contract. Code
// may accept either on the way in, but anything that persists a reference or
// hands one to another system emits the canonical URI — see FormatCanonicalRef.
//
// Everything not on that list fails closed: unknown schemes, unknown object
// kinds, URL decorations (query, fragment, userinfo, port, percent-encoding,
// extra or trailing path segments), malformed node ids, and malformed UUIDs.
// Callers must not pass an unnormalized reference string deeper into routing;
// parse first and carry the tuple.

// RefScheme is the URI scheme for canonical meristem object references.
const RefScheme = "mrs"

// RefKindWorkItems is the canonical URI path segment naming the work-item
// object kind. It is the only object kind that currently has a reference form;
// a URI naming any other kind fails closed rather than being routed as a work
// item.
const RefKindWorkItems = "work-items"

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

// ParseQualifiedRef resolves any accepted reference spelling to its home
// node_id and local UUID. It accepts the canonical URI
// `mrs://<node_id>/work-items/<uuid>`, the compact alias `<node_id>:<uuid>`,
// and a bare `<uuid>`. The first two yield the node_id; a bare UUID means
// "this node" and yields an empty nodeID with ok true. ok is false for
// anything else.
//
// The canonical URI and the compact alias for the same object normalize to
// identical tuples — that equivalence is what lets a caller accept either
// spelling and still route deterministically.
//
// FormatQualifiedRef(ParseQualifiedRef(x)) reproduces x for the bare and
// compact forms. It does not reproduce the canonical URI: that direction is
// FormatCanonicalRef's job, because a compact ref carries no object kind to
// rebuild the path from.
func ParseQualifiedRef(ref string) (nodeID string, id uuid.UUID, ok bool) {
	// A scheme separator means this must be a well-formed canonical URI, and
	// is never retried as a compact ref. Without this branch `http://...` would
	// fall through to the colon cut below, where "http" passes ValidNodeID and
	// the failure would be reported against the UUID rather than the scheme.
	if strings.Contains(ref, "://") {
		return parseCanonicalRef(ref)
	}
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

// parseCanonicalRef validates the full canonical URI form. Every component is
// checked explicitly rather than pattern-matched, because the components a URI
// can carry but this form must not — userinfo, port, query, fragment,
// percent-encoding — are exactly the ones that let two different strings name
// the same object, or one string appear to name a different one.
func parseCanonicalRef(ref string) (nodeID string, id uuid.UUID, ok bool) {
	// The fragment delimiter is checked on the raw string, not on the parsed
	// components, because net/url represents a *trailing bare* '#' with both
	// Fragment and RawFragment empty — the decoration is real but invisible
	// downstream. A component-level check therefore accepts `...<uuid>#` and
	// returns the same tuple as the undecorated form, which is precisely the
	// two-strings-for-one-object failure the canonical form exists to prevent.
	// Rejecting the delimiter outright also covers every populated fragment,
	// so no separate component check is needed.
	if strings.Contains(ref, "#") {
		return "", uuid.Nil, false
	}
	u, err := url.Parse(ref)
	if err != nil {
		return "", uuid.Nil, false
	}
	// url.Parse lowercases the scheme, so a case variant of the scheme is
	// accepted; nothing else about the reference is case-folded.
	if u.Scheme != RefScheme || u.Opaque != "" {
		return "", uuid.Nil, false
	}
	if u.User != nil || u.RawQuery != "" || u.ForceQuery {
		return "", uuid.Nil, false
	}
	// A non-empty RawPath means the escaped path differs from the default
	// encoding of Path — i.e. the sender percent-encoded something. Reject it
	// rather than decode it, so `/work%2ditems/...` cannot smuggle its way to
	// the same tuple as `/work-items/...`.
	if u.RawPath != "" {
		return "", uuid.Nil, false
	}
	// Host carries any port, and ValidNodeID rejects the colon, so an
	// authority like `den:8080` fails here.
	if !ValidNodeID(u.Host) {
		return "", uuid.Nil, false
	}
	kind, rest, found := strings.Cut(strings.TrimPrefix(u.Path, "/"), "/")
	if !found || kind != RefKindWorkItems {
		return "", uuid.Nil, false
	}
	// rest must be the UUID alone. A trailing slash or a further path segment
	// leaves a separator in the string, which uuid.Parse rejects — so
	// `.../work-items/<uuid>/events` and `.../work-items/<uuid>/` both fail
	// here without a separate segment-count guard.
	parsed, err := uuid.Parse(rest)
	if err != nil {
		return "", uuid.Nil, false
	}
	// uuid.Parse is deliberately liberal — it also accepts brace-wrapped,
	// urn-prefixed, and undashed spellings. The compact alias keeps that
	// leniency for compatibility, but the canonical form does not: requiring
	// the standard dashed lowercase spelling is what makes "canonical" mean one
	// string per object, so a durable reference compares equal as text and not
	// only after parsing.
	if parsed.String() != rest {
		return "", uuid.Nil, false
	}
	return u.Host, parsed, true
}

// FormatQualifiedRef renders the compact reference form. An empty nodeID means
// "this node" and produces the bare UUID; a non-empty nodeID produces the
// `<node_id>:<uuid>` alias. The nodeID is assumed valid (ValidNodeID); it is
// the caller's job to validate before formatting.
//
// This is the in-process spelling. Do not use it for anything that leaves the
// node or gets persisted — that is FormatCanonicalRef.
func FormatQualifiedRef(nodeID string, id uuid.UUID) string {
	if nodeID == "" {
		return id.String()
	}
	return nodeID + ":" + id.String()
}

// FormatCanonicalRef renders the canonical work-item reference
// `mrs://<node_id>/work-items/<uuid>`. This is the form external and durable
// surfaces emit.
//
// Unlike FormatQualifiedRef it validates and reports failure instead of
// trusting the caller, and it has no "this node" spelling: the canonical form
// names a home explicitly, so a local object must be formatted with the local
// node's own id rather than an empty one. Both rules exist because the output
// outlives the call — a malformed reference written to an event or handed to a
// peer is not recoverable by fixing the caller later.
func FormatCanonicalRef(nodeID string, id uuid.UUID) (string, bool) {
	if !ValidNodeID(nodeID) {
		return "", false
	}
	return RefScheme + "://" + nodeID + "/" + RefKindWorkItems + "/" + id.String(), true
}
