// Package export produces the publishable corpus: a deterministic,
// privacy-scrubbed JSONL fold over the events table (refresh-requirements
// R8). Raw dumps remain the owner's private diary; this is the projection
// safe to share — the first realized instance of "the system can be
// assessed by being asked": a pure fold over the log.
//
// The exporter is read-only by construction: it holds no events.Writer and
// opens no transaction that writes. Nothing about an export run appears in
// the log it exports.
package export

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/domain"
)

// KindAllowlist enumerates the event kinds the corpus may contain. This is
// a positive policy decision per kind, mirroring the feed's Included/
// Excluded partition discipline: token administration and idempotency
// caching are audit-only; message.captured carries the owner's verbatim
// inbox text and never leaves the private log.
var KindAllowlist = map[string]bool{
	domain.EventWorkItemCreated:             true,
	domain.EventWorkItemTransitioned:        true,
	domain.EventWorkItemEventAppended:       true,
	domain.EventWorkItemRelationAdded:       true,
	domain.EventWorkItemMetadataUpdated:     true,
	domain.EventSignalReceived:              true,
	domain.EventEscalationRequested:         true,
	domain.EventSubactorGrantRequested:      true,
	domain.EventSubactorGrantGranted:        true,
	domain.EventSubactorGrantDenied:         true,
	domain.EventSubactorGrantEscalated:      true,
	domain.EventPatienceBreached:            true,
	domain.EventConvergenceVerdictRecorded:  true,
	domain.EventConvergenceChecksProposed:   true,
	domain.EventPolicyProfileSwitched:       true,
	domain.EventDispatchRequested:           true,
	"tropism.defined":                       true,
	"cultivar.defined":                      true,
	"projection.defined":                    true,
}

// scrubbedFields are free-text payload fields replaced with
// length-preserving markers. Structure survives (reducers and researchers
// can study shape, cadence, and lifecycle); the owner's prose does not.
var scrubbedFields = map[string]bool{
	"title":       true,
	"body":        true,
	"reason":      true,
	"summary":     true,
	"note":        true,
	"notes":       true,
	"description": true,
	"rationale":   true,
	"text":        true,
	"inner":       true,
}

// Record is one exported corpus line.
type Record struct {
	EventID     uuid.UUID `json:"event_id"`
	OccurredAt  time.Time `json:"occurred_at"`
	Source      string    `json:"source"`
	SubjectKind string    `json:"subject_kind"`
	SubjectID   uuid.UUID `json:"subject_id"`
	Kind        string    `json:"kind"`
	Payload     any       `json:"payload"`
}

// Run folds the events table into JSONL on w, oldest first. Only
// allowlisted kinds are emitted; free-text fields are scrubbed recursively.
// Token names never appear because token.* kinds are not allowlisted and
// actor_token_id is deliberately not exported (attribution stays private;
// source human|agent|system is kept for research value).
func Run(ctx context.Context, pool *pgxpool.Pool, w io.Writer) (int, error) {
	rows, err := pool.Query(ctx, `
		SELECT id, occurred_at, source, subject_kind, subject_id, kind, payload
		FROM events
		ORDER BY seq
	`)
	if err != nil {
		return 0, fmt.Errorf("export: query events: %w", err)
	}
	defer rows.Close()

	enc := json.NewEncoder(w)
	count := 0
	for rows.Next() {
		var rec Record
		var payload []byte
		if err := rows.Scan(&rec.EventID, &rec.OccurredAt, &rec.Source, &rec.SubjectKind, &rec.SubjectID, &rec.Kind, &payload); err != nil {
			return count, fmt.Errorf("export: scan: %w", err)
		}
		if !KindAllowlist[rec.Kind] {
			continue
		}
		var decoded any
		if err := json.Unmarshal(payload, &decoded); err != nil {
			return count, fmt.Errorf("export: decode payload for %s: %w", rec.EventID, err)
		}
		rec.Payload = Scrub(decoded)
		if err := enc.Encode(rec); err != nil {
			return count, fmt.Errorf("export: write: %w", err)
		}
		count++
	}
	return count, rows.Err()
}

// Scrub walks a decoded payload and replaces free-text field values with
// length-preserving markers. Nested objects and arrays are walked; keys and
// non-text structure survive untouched.
func Scrub(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			if scrubbedFields[k] {
				out[k] = marker(val)
				continue
			}
			out[k] = Scrub(val)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, val := range t {
			out[i] = Scrub(val)
		}
		return out
	default:
		return v
	}
}

func marker(v any) any {
	s, ok := v.(string)
	if !ok {
		// Non-string free-text slots (e.g. inner objects) are reduced to a
		// shape marker rather than walked: their interior is narrative.
		b, err := json.Marshal(v)
		if err != nil {
			return "[scrubbed]"
		}
		return fmt.Sprintf("[scrubbed %d bytes]", len(b))
	}
	if strings.TrimSpace(s) == "" {
		return s
	}
	return fmt.Sprintf("[scrubbed %d chars]", len(s))
}
