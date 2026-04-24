package events

import (
	"testing"

	"github.com/google/uuid"

	"github.com/jbmopper/wayline/internal/domain"
)

var fixedSubject = uuid.MustParse("11111111-1111-1111-1111-111111111111")

func mkSpec(payload any) Spec {
	return Spec{
		SubjectKind: "work_item",
		SubjectID:   fixedSubject,
		Kind:        "work_item.created",
		Source:      domain.SourceHuman,
		Payload:     payload,
	}
}

func TestDeterministicIDStableAcrossKeyOrder(t *testing.T) {
	a, err := DeterministicID(mkSpec(map[string]any{
		"title": "x",
		"body":  "y",
		"meta":  map[string]any{"a": 1, "b": 2},
	}))
	if err != nil {
		t.Fatal(err)
	}
	b, err := DeterministicID(mkSpec(map[string]any{
		"meta":  map[string]any{"b": 2, "a": 1},
		"body":  "y",
		"title": "x",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Errorf("expected stable id across key order, got %s vs %s", a, b)
	}
}

func TestDeterministicIDDiffersByPayload(t *testing.T) {
	a, _ := DeterministicID(mkSpec(map[string]any{"title": "x"}))
	b, _ := DeterministicID(mkSpec(map[string]any{"title": "y"}))
	if a == b {
		t.Errorf("expected distinct ids for distinct payloads, got %s twice", a)
	}
}

func TestDeterministicIDDiffersByKind(t *testing.T) {
	base := mkSpec(map[string]any{"x": 1})
	a, _ := DeterministicID(base)
	base.Kind = "work_item.transitioned"
	b, _ := DeterministicID(base)
	if a == b {
		t.Errorf("expected distinct ids for distinct kinds, got %s", a)
	}
}

func TestDeterministicIDDiffersBySubject(t *testing.T) {
	base := mkSpec(map[string]any{"x": 1})
	a, _ := DeterministicID(base)
	base.SubjectID = uuid.MustParse("22222222-2222-2222-2222-222222222222")
	b, _ := DeterministicID(base)
	if a == b {
		t.Error("expected distinct ids for distinct subjects")
	}
}

func TestDeterministicIDIgnoresAttribution(t *testing.T) {
	// Attribution is metadata: the same logical event causing the same row
	// in the same projection must collapse on replay even if the second
	// caller is a different token. Otherwise idempotent retries from a
	// rotated token would double-write.
	base := mkSpec(map[string]any{"x": 1})
	a, _ := DeterministicID(base)

	tok := uuid.MustParse("33333333-3333-3333-3333-333333333333")
	base.ActorTokenID = &tok
	base.Source = domain.SourceAgent
	b, _ := DeterministicID(base)

	if a != b {
		t.Errorf("attribution must not influence id, got %s vs %s", a, b)
	}
}

func TestDeterministicIDIsValidUUIDv5Shape(t *testing.T) {
	id, err := DeterministicID(mkSpec(map[string]any{"x": 1}))
	if err != nil {
		t.Fatal(err)
	}
	if id.Version() != 5 {
		t.Errorf("expected version 5 UUID, got version %d", id.Version())
	}
	if id.Variant() != uuid.RFC4122 {
		t.Errorf("expected RFC 4122 variant, got %v", id.Variant())
	}
}

func TestDeterministicIDNilPayloadEqualsEmptyObject(t *testing.T) {
	// Per canonicalJSON's contract, nil and empty object collapse. Two
	// callers — one passing nil, one passing map{} — must produce the same
	// id, otherwise the "no meaningful payload" cases fragment.
	a, err := DeterministicID(mkSpec(nil))
	if err != nil {
		t.Fatal(err)
	}
	b, err := DeterministicID(mkSpec(map[string]any{}))
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Errorf("nil payload should equal empty object, got %s vs %s", a, b)
	}
}

func TestSpecValidate(t *testing.T) {
	subj := uuid.New()
	cases := []struct {
		name    string
		spec    Spec
		wantErr bool
	}{
		{"valid", Spec{SubjectKind: "x", SubjectID: subj, Kind: "x.y", Source: domain.SourceHuman}, false},
		{"missing kind", Spec{SubjectKind: "x", SubjectID: subj, Source: domain.SourceHuman}, true},
		{"missing subject kind", Spec{SubjectID: subj, Kind: "x.y", Source: domain.SourceHuman}, true},
		{"nil subject id", Spec{SubjectKind: "x", Kind: "x.y", Source: domain.SourceHuman}, true},
		{"invalid source", Spec{SubjectKind: "x", SubjectID: subj, Kind: "x.y", Source: "invalid"}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.spec.validate()
			if tc.wantErr && err == nil {
				t.Error("expected validation error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Errorf("expected valid spec, got error: %v", err)
			}
		})
	}
}
