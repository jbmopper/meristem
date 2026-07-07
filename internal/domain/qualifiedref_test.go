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
