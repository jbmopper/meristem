package feed

import (
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// ErrInvalidCursor is returned when a caller-supplied cursor is malformed
// or has the wrong length. The HTTP layer maps it to 400 so consumers
// learn fast that they're round-tripping the wrong string. Silent
// fallback to "from the start" was rejected during e1625848 design
// because it masks consumer bugs.
var ErrInvalidCursor = errors.New("invalid cursor")

// cursor is the SERVER-SIDE shape of an opaque resume token. Consumers
// MUST treat the encoded form as a verbatim blob; the encoding is an
// implementation detail and we reserve the right to change it. v0
// encodes (occurred_at, event_id) as 24 raw bytes (8 BE micros + 16
// UUID) → 32 base64url chars (no padding).
//
// (occurred_at, id) is the compound sort key used by the after-cursor
// query, picked because event_id is content-addressed (SHA-256 of the
// canonical payload) and so has no relationship to time on its own.
// occurred_at can collide on simultaneous appends; id is the
// tie-breaker.
type cursor struct {
	occurredAt time.Time
	eventID    uuid.UUID
}

const cursorRawSize = 8 + 16

func encodeCursor(occurredAt time.Time, eventID uuid.UUID) string {
	buf := make([]byte, cursorRawSize)
	binary.BigEndian.PutUint64(buf[:8], uint64(occurredAt.UnixMicro()))
	copy(buf[8:], eventID[:])
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
		return cursor{}, fmt.Errorf("%w: wrong length", ErrInvalidCursor)
	}
	micros := int64(binary.BigEndian.Uint64(raw[:8]))
	c := cursor{
		occurredAt: time.UnixMicro(micros).UTC(),
	}
	copy(c.eventID[:], raw[8:])
	return c, nil
}
