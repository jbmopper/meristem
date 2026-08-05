package domain

import (
	"testing"

	"github.com/google/uuid"
)

func TestValidNodeID(t *testing.T) {
	valid := []string{"m4", "den", "home-server", "a", "node-1", "abc123",
		"a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4"} // 32 chars
	for _, s := range valid {
		if !ValidNodeID(s) {
			t.Errorf("ValidNodeID(%q) = false, want true", s)
		}
	}
	invalid := []string{
		"",                                  // empty
		"-m4",                               // leading hyphen
		"m4-",                               // trailing hyphen
		"M4",                                // uppercase
		"m_4",                               // underscore
		"m4:den",                            // colon
		"node.one",                          // dot
		"a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d45", // 33 chars
	}
	for _, s := range invalid {
		if ValidNodeID(s) {
			t.Errorf("ValidNodeID(%q) = true, want false", s)
		}
	}
}

func TestParseQualifiedRefQualified(t *testing.T) {
	id := uuid.MustParse("60959376-e0ff-5207-9270-dacfb403333e")
	node, got, ok := ParseQualifiedRef("m4:" + id.String())
	if !ok {
		t.Fatal("ParseQualifiedRef qualified: ok = false, want true")
	}
	if node != "m4" {
		t.Errorf("node = %q, want m4", node)
	}
	if got != id {
		t.Errorf("id = %s, want %s", got, id)
	}
}

func TestParseQualifiedRefUnqualifiedMeansThisNode(t *testing.T) {
	id := uuid.MustParse("60959376-e0ff-5207-9270-dacfb403333e")
	node, got, ok := ParseQualifiedRef(id.String())
	if !ok {
		t.Fatal("ParseQualifiedRef bare uuid: ok = false, want true")
	}
	if node != "" {
		t.Errorf("node = %q, want empty (this node)", node)
	}
	if got != id {
		t.Errorf("id = %s, want %s", got, id)
	}
}

func TestParseQualifiedRefRejectsBadInput(t *testing.T) {
	id := uuid.New().String()
	bad := []string{
		"",               // empty
		"not-a-uuid",     // bare non-uuid
		"m4:not-a-uuid",  // qualified with bad uuid
		":" + id,         // empty node_id
		"m4:",            // empty uuid
		"BAD:" + id,      // uppercase node_id
		"node.one:" + id, // node_id with dot
		"-m4:" + id,      // leading-hyphen node_id
	}
	for _, ref := range bad {
		if _, _, ok := ParseQualifiedRef(ref); ok {
			t.Errorf("ParseQualifiedRef(%q) ok = true, want false", ref)
		}
	}
}

func TestQualifiedRefRoundTrip(t *testing.T) {
	id := uuid.MustParse("60959376-e0ff-5207-9270-dacfb403333e")
	for _, ref := range []string{
		id.String(),          // bare (this node) round-trips unchanged
		"m4:" + id.String(),  // qualified round-trips unchanged
		"den:" + id.String(), // another node
	} {
		node, parsed, ok := ParseQualifiedRef(ref)
		if !ok {
			t.Fatalf("ParseQualifiedRef(%q): ok = false", ref)
		}
		if got := FormatQualifiedRef(node, parsed); got != ref {
			t.Errorf("round-trip %q: got %q", ref, got)
		}
	}
}

func TestFormatQualifiedRef(t *testing.T) {
	id := uuid.MustParse("60959376-e0ff-5207-9270-dacfb403333e")
	if got := FormatQualifiedRef("", id); got != id.String() {
		t.Errorf("FormatQualifiedRef(this node) = %q, want %q", got, id.String())
	}
	if got := FormatQualifiedRef("den", id); got != "den:"+id.String() {
		t.Errorf("FormatQualifiedRef(den) = %q, want %q", got, "den:"+id.String())
	}
}

// TestCanonicalRefRoundTrip pins that the canonical URI survives a
// format/parse/format cycle unchanged. This is the form that gets persisted and
// handed to peers, so a reference that drifts on round-trip would mean the same
// object accumulates more than one durable spelling.
func TestCanonicalRefRoundTrip(t *testing.T) {
	id := uuid.MustParse("60959376-e0ff-5207-9270-dacfb403333e")
	ref, ok := FormatCanonicalRef("den", id)
	if !ok {
		t.Fatal("FormatCanonicalRef(den): ok = false, want true")
	}
	if want := "mrs://den/work-items/" + id.String(); ref != want {
		t.Fatalf("FormatCanonicalRef = %q, want %q", ref, want)
	}
	node, parsed, ok := ParseQualifiedRef(ref)
	if !ok {
		t.Fatalf("ParseQualifiedRef(%q): ok = false, want true", ref)
	}
	if node != "den" || parsed != id {
		t.Fatalf("parsed = (%q, %s), want (den, %s)", node, parsed, id)
	}
	again, ok := FormatCanonicalRef(node, parsed)
	if !ok || again != ref {
		t.Fatalf("re-format = (%q, %v), want (%q, true)", again, ok, ref)
	}
}

// TestCompactAliasAndCanonicalURINormalizeIdentically is the equivalence the
// whole two-spelling design rests on: a caller may accept either form and must
// route to the same place. If these ever diverge, the compact alias stops being
// an alias and becomes a second, silently different naming scheme.
func TestCompactAliasAndCanonicalURINormalizeIdentically(t *testing.T) {
	id := uuid.MustParse("60959376-e0ff-5207-9270-dacfb403333e")
	for _, node := range []string{"den", "m4", "home-server"} {
		compact := FormatQualifiedRef(node, id)
		canonical, ok := FormatCanonicalRef(node, id)
		if !ok {
			t.Fatalf("FormatCanonicalRef(%q): ok = false", node)
		}
		cNode, cID, cOK := ParseQualifiedRef(compact)
		uNode, uID, uOK := ParseQualifiedRef(canonical)
		if !cOK || !uOK {
			t.Fatalf("%s: compact ok = %v, canonical ok = %v, want both true", node, cOK, uOK)
		}
		if cNode != uNode || cID != uID {
			t.Fatalf("%s: compact -> (%q, %s) but canonical -> (%q, %s); spellings must normalize identically",
				node, cNode, cID, uNode, uID)
		}
		if cNode != node || cID != id {
			t.Fatalf("%s: normalized to (%q, %s), want (%q, %s)", node, cNode, cID, node, id)
		}
	}
}

// TestBareUUIDStaysLocalAgainstCanonicalForm restates the local rule alongside
// the URI form. An unqualified UUID is always the interpreting node's own
// object and must never acquire a home from parsing — that rule is what keeps
// every existing local caller correct now that remote spellings exist.
func TestBareUUIDStaysLocalAgainstCanonicalForm(t *testing.T) {
	id := uuid.MustParse("60959376-e0ff-5207-9270-dacfb403333e")
	node, parsed, ok := ParseQualifiedRef(id.String())
	if !ok {
		t.Fatal("bare uuid: ok = false, want true")
	}
	if node != "" {
		t.Fatalf("bare uuid resolved to node %q, want local (empty)", node)
	}
	if parsed != id {
		t.Fatalf("bare uuid id = %s, want %s", parsed, id)
	}
	// And a local ref has no canonical spelling without an explicit home.
	if _, ok := FormatCanonicalRef("", id); ok {
		t.Fatal("FormatCanonicalRef with empty nodeID: ok = true, want false — the canonical form must name a home")
	}
}

// TestCanonicalRefFailsClosed covers the refusal contract. Each case is a
// string that could plausibly be routed as a work-item reference by a looser
// parser; all of them must be rejected outright rather than coerced, because
// the failure mode is dispatching to the wrong node or the wrong object.
func TestCanonicalRefFailsClosed(t *testing.T) {
	id := uuid.MustParse("60959376-e0ff-5207-9270-dacfb403333e").String()
	bad := map[string]string{
		"wrong scheme":             "http://den/work-items/" + id,
		"unknown scheme":           "meristem://den/work-items/" + id,
		"wrong object kind":        "mrs://den/tropisms/" + id,
		"missing object kind":      "mrs://den/" + id,
		"extra path segment":       "mrs://den/work-items/" + id + "/events",
		"trailing slash":           "mrs://den/work-items/" + id + "/",
		"query string":             "mrs://den/work-items/" + id + "?replica=2",
		"bare query marker":        "mrs://den/work-items/" + id + "?",
		"fragment":                 "mrs://den/work-items/" + id + "#top",
		"userinfo":                 "mrs://operator@den/work-items/" + id,
		"userinfo with password":   "mrs://operator:secret@den/work-items/" + id,
		"port":                     "mrs://den:8080/work-items/" + id,
		"percent-encoded path":     "mrs://den/work%2ditems/" + id,
		"empty host":               "mrs:///work-items/" + id,
		"uppercase host":           "mrs://DEN/work-items/" + id,
		"dotted host":              "mrs://den.example/work-items/" + id,
		"leading-hyphen host":      "mrs://-den/work-items/" + id,
		"malformed uuid":           "mrs://den/work-items/not-a-uuid",
		"empty uuid":               "mrs://den/work-items/",
		"brace-wrapped uuid":       "mrs://den/work-items/{" + id + "}",
		"undashed uuid":            "mrs://den/work-items/60959376e0ff52079270dacfb403333e",
		"uppercase uuid":           "mrs://den/work-items/60959376-E0FF-5207-9270-DACFB403333E",
		"scheme without authority": "mrs:den/work-items/" + id,
		"path-only":                "/work-items/" + id,
		"double scheme separator":  "mrs://mrs://den/work-items/" + id,
	}
	for name, ref := range bad {
		if node, parsed, ok := ParseQualifiedRef(ref); ok {
			t.Errorf("%s: ParseQualifiedRef(%q) = (%q, %s, true), want ok = false", name, ref, node, parsed)
		}
	}
}

// TestCanonicalRefSchemeIsCaseInsensitive pins the one case fold the parser
// does perform. URI schemes are case-insensitive by RFC 3986 and url.Parse
// lowercases them; nothing else in a reference is folded, which is why the
// uppercase host and uppercase UUID cases above are rejected.
func TestCanonicalRefSchemeIsCaseInsensitive(t *testing.T) {
	id := uuid.MustParse("60959376-e0ff-5207-9270-dacfb403333e")
	for _, ref := range []string{
		"MRS://den/work-items/" + id.String(),
		"Mrs://den/work-items/" + id.String(),
	} {
		node, parsed, ok := ParseQualifiedRef(ref)
		if !ok {
			t.Errorf("ParseQualifiedRef(%q): ok = false, want true", ref)
			continue
		}
		if node != "den" || parsed != id {
			t.Errorf("ParseQualifiedRef(%q) = (%q, %s), want (den, %s)", ref, node, parsed, id)
		}
	}
}

// TestFormatCanonicalRefValidatesNodeID pins that the emitter refuses to build
// a reference it could not parse back. FormatQualifiedRef trusts its caller;
// this one must not, because its output is what gets persisted.
func TestFormatCanonicalRefValidatesNodeID(t *testing.T) {
	id := uuid.MustParse("60959376-e0ff-5207-9270-dacfb403333e")
	for _, node := range []string{"", "DEN", "den.example", "-den", "den-", "den:8080", "de n"} {
		if ref, ok := FormatCanonicalRef(node, id); ok {
			t.Errorf("FormatCanonicalRef(%q) = (%q, true), want ok = false", node, ref)
		}
	}
}
