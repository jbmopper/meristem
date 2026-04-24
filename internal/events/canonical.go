package events

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
)

// CanonicalJSON returns a deterministic JSON encoding of v: object keys are
// sorted lexicographically at every level of nesting, and integer precision
// is preserved (no silent float64 widening).
//
// The same logical value produces the same bytes regardless of map iteration
// order, struct field order, or how the caller built the value. This is the
// substrate the deterministic event id depends on; if two payloads canonicalize
// to the same bytes, they *must* produce the same id, by construction.
//
// nil and JSON `null` both canonicalize to `{}`. The empty object is the
// least surprising default for an event without a meaningful payload, and it
// keeps the events.payload column (NOT NULL JSONB DEFAULT '{}'::jsonb) happy
// without introducing a separate nullable code path.
func CanonicalJSON(v any) ([]byte, error) {
	if v == nil {
		return []byte("{}"), nil
	}

	raw, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("canonical: marshal: %w", err)
	}
	if string(raw) == "null" {
		return []byte("{}"), nil
	}

	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var generic any
	if err := dec.Decode(&generic); err != nil {
		return nil, fmt.Errorf("canonical: decode: %w", err)
	}

	var buf bytes.Buffer
	if err := writeCanonical(&buf, generic); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func writeCanonical(buf *bytes.Buffer, v any) error {
	switch x := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		buf.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				buf.WriteByte(',')
			}
			kb, err := json.Marshal(k)
			if err != nil {
				return err
			}
			buf.Write(kb)
			buf.WriteByte(':')
			if err := writeCanonical(buf, x[k]); err != nil {
				return err
			}
		}
		buf.WriteByte('}')
	case []any:
		buf.WriteByte('[')
		for i, e := range x {
			if i > 0 {
				buf.WriteByte(',')
			}
			if err := writeCanonical(buf, e); err != nil {
				return err
			}
		}
		buf.WriteByte(']')
	case json.Number:
		buf.WriteString(string(x))
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return err
		}
		buf.Write(b)
	}
	return nil
}
