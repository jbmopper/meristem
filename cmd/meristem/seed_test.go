package main

import (
	"testing"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/domain"
)

func TestSlugify(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"", ""},
		{"hello world", "hello-world"},
		{"  HELLO   World  ", "hello-world"},
		{"work_item, message, artifact", "work-item-message-artifact"},
		{"POST /v1/inbox/messages", "post-v1-inbox-messages"},
		{"!!!---!!!", ""},
		{"foo--bar", "foo-bar"},
		{"trailing!!!", "trailing"},
	}
	for _, c := range cases {
		if got := slugify(c.in); got != c.want {
			t.Errorf("slugify(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// TestSeedSubjectID_Stable pins the deterministic id for a known title.
// This is the durable contract: if anyone changes seedNamespace or
// slugify, the assertion below must change too — and the project owner
// has to consciously accept that every previously-seeded work_item is
// being re-seeded under a new id.
func TestSeedSubjectID_Stable(t *testing.T) {
	got := seedSubjectID("Webhook verification")
	want := uuid.NewSHA1(seedNamespace, []byte("webhook-verification"))
	if got != want {
		t.Errorf("seedSubjectID drift: got %s, want %s", got, want)
	}
	// Sanity-check: the namespace itself must be stable.
	if seedNamespace.String() != "4d6f3a3a-1ce8-5f6b-8b01-7a64c1e0a3a2" {
		t.Errorf("seedNamespace was changed; treat that as a re-seed and document why")
	}
}

func TestV1SubstrateItems_NonEmptyAndUnique(t *testing.T) {
	if len(v1SubstrateItems) == 0 {
		t.Fatal("v1SubstrateItems is empty")
	}
	seenSlug := make(map[string]string, len(v1SubstrateItems))
	seenID := make(map[uuid.UUID]string, len(v1SubstrateItems))
	for _, item := range v1SubstrateItems {
		if item.Title == "" {
			t.Errorf("item with empty title: %+v", item)
		}
		if item.Body == "" {
			t.Errorf("item %q has empty body", item.Title)
		}
		slug := slugify(item.Title)
		if slug == "" {
			t.Errorf("item %q slugifies to empty string", item.Title)
		}
		if prev, ok := seenSlug[slug]; ok {
			t.Errorf("duplicate slug %q for items %q and %q", slug, prev, item.Title)
		}
		seenSlug[slug] = item.Title

		id := seedSubjectID(item.Title)
		if prev, ok := seenID[id]; ok {
			t.Errorf("duplicate subject_id %s for items %q and %q", id, prev, item.Title)
		}
		seenID[id] = item.Title
	}
}

// TestV1SubstrateItems_Fingerprint pins the in-binary item list so an
// accidental edit is loud. Update the constant deliberately when adding
// or rewording an item; renaming a title is a re-seed (new subject_id),
// so do it consciously.
func TestV1SubstrateItems_Fingerprint(t *testing.T) {
	const want = "e7c0c4edadab94d9684a8d03f111fd6ade274254d6042cdf2c9edd6f913915a0"
	got := seedItemsFingerprint()
	if got != want {
		t.Errorf("v1SubstrateItems fingerprint drift:\n  got:  %s\n  want: %s\n\n"+
			"If this change is intentional, update the constant. If not, you are about to "+
			"either re-seed an existing item under a new id (title changed) or fork its "+
			"event row (body changed without title changing). Both are surprising.",
			got, want)
	}
}

func TestV1SubstrateItems_CountMatchesSpec(t *testing.T) {
	// docs/spec.md §v1 Substrate has exactly 18 bullets. If you add or
	// remove one in spec.md, mirror it here and bump this constant.
	const want = 18
	if got := len(v1SubstrateItems); got != want {
		t.Errorf("expected %d v1 substrate items (matching docs/spec.md §v1 Substrate), got %d", want, got)
	}
}

func TestProjectionSeedDefinitionsIncludeDispatch(t *testing.T) {
	for _, item := range projectionSeedDefinitions {
		if item.Name != "dispatch" {
			continue
		}
		if !item.Rootstock {
			t.Fatal("dispatch projection must be rootstock")
		}
		if len(item.Filter.Kinds) != 1 || item.Filter.Kinds[0] != domain.EventDispatchRequested {
			t.Fatalf("dispatch filter = %+v, want [%s]", item.Filter, domain.EventDispatchRequested)
		}
		return
	}
	t.Fatal("projection seed definitions missing dispatch")
}
