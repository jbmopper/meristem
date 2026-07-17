package feed

import (
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/domain"
)

func TestCursorIdentityBindsFilterFingerprint(t *testing.T) {
	tokenID := uuid.New()
	filtered, err := NormalizeReadFilter(ReadFilter{Predicates: []Predicate{
		{Kind: PredicateExcludeActor, TokenID: tokenID},
	}})
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	fp := filtered.FingerprintHash()
	if fp == "" {
		t.Fatal("filtered fingerprint is empty")
	}
	if (ReadFilter{}).FingerprintHash() != "" {
		t.Fatal("empty filter has a fingerprint")
	}

	issued := encodeCursorFor(42, "", 0, fp)
	decoded, err := decodeCursorForIdentity(issued, "", 0, fp)
	if err != nil || decoded.seq != 42 || decoded.filter != fp {
		t.Fatalf("round-trip: %+v err=%v", decoded, err)
	}

	if _, err := decodeCursorForIdentity(issued, "", 0, ""); !errors.Is(err, ErrCursorFilterMismatch) {
		t.Fatalf("filtered cursor on plain read = %v, want ErrCursorFilterMismatch", err)
	}
	plain := encodeCursorFor(42, "", 0, "")
	if _, err := decodeCursorForIdentity(plain, "", 0, fp); !errors.Is(err, ErrCursorFilterMismatch) {
		t.Fatalf("plain cursor on filtered read = %v, want ErrCursorFilterMismatch", err)
	}

	other, err := NormalizeReadFilter(ReadFilter{Predicates: []Predicate{
		{Kind: PredicateActor, TokenID: tokenID},
	}})
	if err != nil {
		t.Fatalf("normalize other: %v", err)
	}
	if other.FingerprintHash() == fp {
		t.Fatal("different filters share a fingerprint")
	}
	if _, err := decodeCursorForIdentity(issued, "", 0, other.FingerprintHash()); !errors.Is(err, ErrCursorFilterMismatch) {
		t.Fatalf("cross-filter resume = %v, want ErrCursorFilterMismatch", err)
	}

	projected := encodeCursorFor(7, "activity", 3, fp)
	if decoded, err := decodeCursorForIdentity(projected, "activity", 3, fp); err != nil || decoded.version != 3 || decoded.filter != fp {
		t.Fatalf("projection+filter round-trip: %+v err=%v", decoded, err)
	}
	if _, err := decodeCursorForIdentity(projected, "owner-attention", 1, fp); !errors.Is(err, ErrCursorProjectionMismatch) {
		t.Fatalf("projection mismatch = %v, want ErrCursorProjectionMismatch", err)
	}
	if _, err := decodeCursorForIdentity(projected, "activity", 3, ""); !errors.Is(err, ErrCursorFilterMismatch) {
		t.Fatalf("projected filter mismatch = %v, want ErrCursorFilterMismatch", err)
	}

	// Assigned-lane fingerprints are caller-specific: two readers never
	// share a channel identity by accident.
	a, _ := NormalizeReadFilter(ReadFilter{Predicates: []Predicate{{Kind: PredicateAssignedOrAddressed, TokenID: uuid.New()}}})
	b, _ := NormalizeReadFilter(ReadFilter{Predicates: []Predicate{{Kind: PredicateAssignedOrAddressed, TokenID: uuid.New()}}})
	if a.FingerprintHash() == b.FingerprintHash() {
		t.Fatal("distinct assigned readers share a fingerprint")
	}
	_ = domain.EventMessageCaptured
}
