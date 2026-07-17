package feed

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
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

// ErrCursorProjectionMismatch is returned when a cursor issued for one feed
// projection is replayed against another projection, or against the default
// unprojected feed. Projection cursors are deliberately scoped so a consumer
// cannot silently skip or replay history by mixing feeds.
var ErrCursorProjectionMismatch = errors.New("cursor projection mismatch")

// ErrCursorFilterMismatch is returned when a cursor issued under one filter
// identity (the canonical predicate fingerprint) is replayed under another —
// including a plain cursor replayed against a filtered read or vice versa.
// Failing closed keeps a resumed stream from silently widening or narrowing;
// the consumer re-bootstraps under the identity it actually holds.
var ErrCursorFilterMismatch = errors.New("cursor filter mismatch")

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
	seq        int64
	projection string
	version    int
	filter     string
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

type projectionCursorEnvelope struct {
	Version    int    `json:"v"`
	Seq        int64  `json:"seq"`
	Projection string `json:"projection,omitempty"`
	Definition int    `json:"definition,omitempty"`
	Filter     string `json:"filter,omitempty"`
}

func encodeIdentityCursor(seq int64, projection string, version int, filter string) string {
	raw, _ := json.Marshal(projectionCursorEnvelope{
		Version:    1,
		Seq:        seq,
		Projection: projection,
		Definition: version,
		Filter:     filter,
	})
	return base64.RawURLEncoding.EncodeToString(raw)
}

// decodeCursorForIdentity enforces the full channel identity: (projection
// name, projection version, canonical predicate fingerprint). Every mismatch
// direction fails closed, including plain-vs-filtered in both orders.
func decodeCursorForIdentity(s string, projection string, version int, filter string) (cursor, error) {
	if projection == "" && filter == "" {
		decoded, err := decodeCursor(s)
		if err == nil {
			return decoded, nil
		}
		scoped, scopedErr := decodeProjectionCursor(s)
		if scopedErr == nil {
			if scoped.projection != "" {
				return cursor{}, fmt.Errorf("%w: cursor belongs to projection %q", ErrCursorProjectionMismatch, scoped.projection)
			}
			if scoped.filter != "" {
				return cursor{}, fmt.Errorf("%w: cursor was issued under a filter identity", ErrCursorFilterMismatch)
			}
		}
		return cursor{}, err
	}
	scoped, err := decodeProjectionCursor(s)
	if err != nil {
		if _, legacyErr := decodeCursor(s); legacyErr == nil {
			if projection != "" {
				return cursor{}, fmt.Errorf("%w: unprojected cursor cannot be used with projection %q", ErrCursorProjectionMismatch, projection)
			}
			return cursor{}, fmt.Errorf("%w: unfiltered cursor cannot resume a filtered read", ErrCursorFilterMismatch)
		}
		return cursor{}, err
	}
	if scoped.projection != projection || scoped.version != version {
		return cursor{}, fmt.Errorf("%w: cursor belongs to %s@%d, requested %s@%d",
			ErrCursorProjectionMismatch, scoped.projection, scoped.version, projection, version)
	}
	if scoped.filter != filter {
		return cursor{}, fmt.Errorf("%w: cursor was issued under a different filter identity", ErrCursorFilterMismatch)
	}
	return scoped, nil
}

func decodeProjectionCursor(s string) (cursor, error) {
	if s == "" {
		return cursor{}, ErrInvalidCursor
	}
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return cursor{}, fmt.Errorf("%w: %v", ErrInvalidCursor, err)
	}
	var env projectionCursorEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return cursor{}, fmt.Errorf("%w: %v", ErrInvalidCursor, err)
	}
	if env.Version != 1 {
		return cursor{}, fmt.Errorf("%w: unsupported projection cursor version %d", ErrInvalidCursor, env.Version)
	}
	if env.Seq < 0 {
		return cursor{}, fmt.Errorf("%w: negative seq", ErrInvalidCursor)
	}
	if env.Projection == "" && env.Filter == "" {
		return cursor{}, fmt.Errorf("%w: missing channel identity", ErrInvalidCursor)
	}
	if env.Projection != "" && env.Definition < 1 {
		return cursor{}, fmt.Errorf("%w: missing projection identity", ErrInvalidCursor)
	}
	return cursor{seq: env.Seq, projection: env.Projection, version: env.Definition, filter: env.Filter}, nil
}
