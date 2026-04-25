package feed

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestCursorRoundtrip(t *testing.T) {
	want := struct {
		t  time.Time
		id uuid.UUID
	}{
		t:  time.Date(2026, 4, 25, 12, 34, 56, 789000000, time.UTC),
		id: uuid.MustParse("11111111-2222-3333-4444-555555555555"),
	}
	enc := encodeCursor(want.t, want.id)
	if enc == "" {
		t.Fatal("encodeCursor returned empty")
	}
	dec, err := decodeCursor(enc)
	if err != nil {
		t.Fatalf("decodeCursor: %v", err)
	}
	if !dec.occurredAt.Equal(want.t) {
		t.Errorf("occurredAt: want %v, got %v", want.t, dec.occurredAt)
	}
	if dec.eventID != want.id {
		t.Errorf("eventID: want %v, got %v", want.id, dec.eventID)
	}
}

// TestCursorOpaqueLength pins the v0 encoded length so consumers don't
// quietly start parsing structure. If the encoding changes, this test
// fails and forces a deliberate version bump conversation.
func TestCursorOpaqueLength(t *testing.T) {
	c := encodeCursor(time.Now(), uuid.New())
	if got := len(c); got != 32 {
		t.Errorf("v0 cursor must be 32 chars, got %d (%q)", got, c)
	}
}

func TestDecodeCursorRejectsMalformed(t *testing.T) {
	cases := map[string]string{
		"empty":           "",
		"not base64":      "not base64!!!",
		"too short":       "AAAA",
		"too long":        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"wrong-len bytes": "0123456789abcdef0123456789abc",
	}
	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := decodeCursor(in)
			if !errors.Is(err, ErrInvalidCursor) {
				t.Errorf("want ErrInvalidCursor, got %v", err)
			}
		})
	}
}

// TestCursorPreservesMicrosecondPrecision pins that the encoding does
// not silently truncate to seconds or milliseconds. Postgres timestamptz
// has microsecond precision; coarser encoding here would fail the
// after-cursor compound comparison for events that share a second.
func TestCursorPreservesMicrosecondPrecision(t *testing.T) {
	t1 := time.Date(2026, 4, 25, 0, 0, 0, 100000, time.UTC)
	t2 := time.Date(2026, 4, 25, 0, 0, 0, 200000, time.UTC)
	id := uuid.New()

	dec1, err := decodeCursor(encodeCursor(t1, id))
	if err != nil {
		t.Fatalf("decode t1: %v", err)
	}
	dec2, err := decodeCursor(encodeCursor(t2, id))
	if err != nil {
		t.Fatalf("decode t2: %v", err)
	}
	if !dec1.occurredAt.Before(dec2.occurredAt) {
		t.Errorf("microsecond precision lost: %v not before %v", dec1.occurredAt, dec2.occurredAt)
	}
}
