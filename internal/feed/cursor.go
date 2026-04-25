package feed

import (
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
)

// ErrInvalidCursor is returned when a caller-supplied cursor is malformed
// (wrong length, not base64) OR semantically invalid (decodes cleanly but
// references a sequence value that was never issued by this server). The
// HTTP layer maps it to 400 so consumers learn fast that they're round-
// tripping the wrong string. Silent fallback to "from the start" was
// rejected during e1625848 design because it masks consumer bugs; the
// existence-validation path was added in the 70a2f732 repair after B
// found that fabricated-but-syntactically-valid v0 cursors were silently
// returning empty pages.
var ErrInvalidCursor = errors.New("invalid cursor")

// cursor is the SERVER-SIDE shape of an opaque resume token. Consumers
// MUST treat the encoded form as a verbatim blob; the encoding is an
// implementation detail and we reserve the right to change it.
//
// v1 (current): encodes a single events.seq value (BIGSERIAL, monotonic
// per-insert) as 8 BE bytes → 11 base64url chars (no padding). seq is
// the right primitive because it is strictly increasing per insert and
// has no contention behavior under same-microsecond writes (which the
// v0 (occurred_at, id) compound key did — see 70a2f732 for the no-skip
// repair narrative).
//
// v0 (legacy, invalidated by this version): 24 raw bytes (8 BE
// occurred_at micros + 16 UUID) → 32 base64url chars. Watchers holding
// a v0 cursor will fail at the length check below and the watcher
// consumer (db27a9c9) will re-bootstrap via its isInvalidCursorErr
// recovery path.
type cursor struct {
	seq int64
}

const cursorRawSize = 8

// CursorEncodedLen is the byte length of a v1 encoded cursor. Pinned as
// a constant so the test that guards "consumers will get fail-fast on
// v0 cursors" can assert against this rather than a magic number.
const CursorEncodedLen = 11

func encodeCursor(seq int64) string {
	buf := make([]byte, cursorRawSize)
	binary.BigEndian.PutUint64(buf, uint64(seq))
	return base64.RawURLEncoding.EncodeToString(buf)
}

func decodeCursor(s string) (cursor, error) {
	if s == "" {
		return cursor{}, ErrInvalidCursor
	}
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return cursor{}, fmt.Errorf("%w: %v", ErrInvalidCursor, err)
	}
	if len(raw) != cursorRawSize {
		return cursor{}, fmt.Errorf("%w: wrong length (got %d bytes, want %d)", ErrInvalidCursor, len(raw), cursorRawSize)
	}
	seq := int64(binary.BigEndian.Uint64(raw))
	if seq < 0 {
		return cursor{}, fmt.Errorf("%w: negative seq", ErrInvalidCursor)
	}
	return cursor{seq: seq}, nil
}
