package feed

import (
	"errors"
	"testing"
)

func TestCursorRoundtrip(t *testing.T) {
	cases := []int64{0, 1, 42, 186, 1 << 30, 1 << 40, (1 << 62) - 1}
	for _, seq := range cases {
		enc := encodeCursor(seq)
		if enc == "" {
			t.Fatalf("encodeCursor(%d) returned empty", seq)
		}
		dec, err := decodeCursor(enc)
		if err != nil {
			t.Fatalf("decodeCursor(%q): %v", enc, err)
		}
		if dec.seq != seq {
			t.Errorf("seq round-trip: want %d, got %d", seq, dec.seq)
		}
	}
}

// TestCursorOpaqueLength pins the v1 encoded length so consumers don't
// quietly start parsing structure. If the encoding changes again, this
// test fails and forces a deliberate version bump conversation, plus
// alerts whoever is reviewing that the watcher consumer's re-bootstrap
// path is the only thing keeping the running fleet from breaking.
func TestCursorOpaqueLength(t *testing.T) {
	c := encodeCursor(186)
	if got := len(c); got != CursorEncodedLen {
		t.Errorf("v1 cursor must be %d chars, got %d (%q)", CursorEncodedLen, got, c)
	}
}

// TestCursorInvalidatesV0Format pins that the v0 32-char cursor format
// (24 raw bytes = 8B occurred_at + 16B UUID) is rejected at the length
// check, NOT silently parsed as a different value. This is what lets
// the watcher consumer recover via its isInvalidCursorErr re-bootstrap
// path without a forced restart of the fleet during the v0 → v1 cutover.
func TestCursorInvalidatesV0Format(t *testing.T) {
	v0Sized := "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" // 32 base64url chars → 24 raw bytes
	if len(v0Sized) != 32 {
		t.Fatalf("test fixture wrong: want 32 chars, got %d", len(v0Sized))
	}
	_, err := decodeCursor(v0Sized)
	if !errors.Is(err, ErrInvalidCursor) {
		t.Errorf("v0-sized cursor should fail at length check, got: %v", err)
	}
}

func TestDecodeCursorRejectsMalformed(t *testing.T) {
	cases := map[string]string{
		"empty":           "",
		"not base64":      "not base64!!!",
		"too short":       "AAAA",
		"too long":        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"v0-sized 32char": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
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

// TestCursorMonotonicEncoding pins that bytewise comparison of encoded
// cursors matches numeric seq comparison. This isn't strictly required
// (consumers don't compare cursors), but it's a useful invariant — and
// debugging is much friendlier when "later cursor sorts after earlier
// cursor" holds in any string-ordered context (logs, dashboards, etc.).
func TestCursorMonotonicEncoding(t *testing.T) {
	earlier := encodeCursor(100)
	later := encodeCursor(200)
	if !(earlier < later) {
		t.Errorf("encoded cursors should be lexicographically monotonic with seq: %q !< %q", earlier, later)
	}
}

func TestProjectionCursorScopesProjectionIdentity(t *testing.T) {
	c := encodeIdentityCursor(42, "activity", 1, "")
	decoded, err := decodeCursorForIdentity(c, "activity", 1, "")
	if err != nil {
		t.Fatalf("decode projection cursor: %v", err)
	}
	if decoded.seq != 42 || decoded.projection != "activity" || decoded.version != 1 {
		t.Fatalf("decoded cursor = %+v", decoded)
	}
	if _, err := decodeCursorForIdentity(c, "owner-attention", 1, ""); !errors.Is(err, ErrCursorProjectionMismatch) {
		t.Fatalf("expected projection mismatch for another projection, got %v", err)
	}
	if _, err := decodeCursorForIdentity(c, "", 0, ""); !errors.Is(err, ErrCursorProjectionMismatch) {
		t.Fatalf("expected projection mismatch on default feed, got %v", err)
	}
	if _, err := decodeCursorForIdentity(encodeCursor(42), "activity", 1, ""); !errors.Is(err, ErrCursorProjectionMismatch) {
		t.Fatalf("expected mismatch when using default cursor on projection, got %v", err)
	}
}
