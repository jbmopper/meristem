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
	"bufio"
	"bytes"
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
	domain.EventWorkItemCreated:            true,
	domain.EventWorkItemTransitioned:       true,
	domain.EventWorkItemEventAppended:      true,
	domain.EventWorkItemRelationAdded:      true,
	domain.EventWorkItemMetadataUpdated:    true,
	domain.EventSignalReceived:             true,
	domain.EventEscalationRequested:        true,
	domain.EventSubactorGrantRequested:     true,
	domain.EventSubactorGrantGranted:       true,
	domain.EventSubactorGrantDenied:        true,
	domain.EventSubactorGrantEscalated:     true,
	domain.EventPatienceBreached:           true,
	domain.EventConvergenceVerdictRecorded: true,
	domain.EventConvergenceChecksProposed:  true,
	domain.EventPolicyProfileSwitched:      true,
	domain.EventDispatchRequested:          true,
	"tropism.defined":                      true,
	"cultivar.defined":                     true,
	"projection.defined":                   true,
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

// ValidationReport is safe to print or commit. It records only counts and
// leak classes, never the private token names or message bodies it checked.
type ValidationReport struct {
	EventsExported       int  `json:"events_exported"`
	LinesChecked         int  `json:"lines_checked"`
	TokenNamesChecked    int  `json:"token_names_checked"`
	MessageBodiesChecked int  `json:"message_bodies_checked"`
	NonAllowlistedKinds  int  `json:"non_allowlisted_kinds"`
	ActorTokenIDLeaks    int  `json:"actor_token_id_leaks"`
	TokenNameLeaks       int  `json:"token_name_leaks"`
	MessageBodyLeaks     int  `json:"message_body_leaks"`
	Valid                bool `json:"valid"`
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

// Validate runs the exporter, then independently checks the generated corpus
// against private strings still present in the restored database. This is the
// operator-facing R8 archive check: restore a private dump into scratch
// Postgres, point MERISTEM_DATABASE_URL at it, and run `meristem export
// --validate`. The returned report is safe to publish because it contains
// counts only.
func Validate(ctx context.Context, pool *pgxpool.Pool) (ValidationReport, error) {
	var buf bytes.Buffer
	eventsExported, err := Run(ctx, pool, &buf)
	if err != nil {
		return ValidationReport{}, err
	}
	tokenNames, err := privateStrings(ctx, pool, `SELECT name FROM tokens WHERE COALESCE(name, '') <> ''`)
	if err != nil {
		return ValidationReport{}, fmt.Errorf("export validation: read token names: %w", err)
	}
	messageBodies, err := privateStrings(ctx, pool, `
		SELECT payload->>'text'
		FROM events
		WHERE kind = $1
		  AND COALESCE(payload->>'text', '') <> ''
	`, domain.EventMessageCaptured)
	if err != nil {
		return ValidationReport{}, fmt.Errorf("export validation: read message bodies: %w", err)
	}

	report, err := ValidateCorpus(buf.Bytes(), tokenNames, messageBodies)
	report.EventsExported = eventsExported
	report.TokenNamesChecked = len(tokenNames)
	report.MessageBodiesChecked = len(messageBodies)
	return report, err
}

// ValidateCorpus is separated from Validate so the leak detector can be tested
// without a database. tokenNames and messageBodies are private inputs and must
// never be copied into the returned report or error.
func ValidateCorpus(corpus []byte, tokenNames, messageBodies []string) (ValidationReport, error) {
	report := ValidationReport{
		TokenNamesChecked:    len(tokenNames),
		MessageBodiesChecked: len(messageBodies),
	}
	scanner := bufio.NewScanner(bytes.NewReader(corpus))
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		report.LinesChecked++
		var raw map[string]any
		if err := json.Unmarshal(line, &raw); err != nil {
			return report, fmt.Errorf("export validation: decode corpus line %d: %w", report.LinesChecked, err)
		}
		kind, _ := raw["kind"].(string)
		if !KindAllowlist[kind] {
			report.NonAllowlistedKinds++
		}
		if _, ok := raw["actor_token_id"]; ok {
			report.ActorTokenIDLeaks++
		}
		exportedStrings := collectStrings(raw)
		report.TokenNameLeaks += leakedPrivateStringCount(exportedStrings, tokenNames)
		report.MessageBodyLeaks += leakedPrivateStringCount(exportedStrings, messageBodies)
	}
	if err := scanner.Err(); err != nil {
		return report, fmt.Errorf("export validation: scan corpus: %w", err)
	}
	if report.NonAllowlistedKinds > 0 ||
		report.ActorTokenIDLeaks > 0 ||
		report.TokenNameLeaks > 0 ||
		report.MessageBodyLeaks > 0 {
		report.Valid = false
		return report, fmt.Errorf("export validation failed: non_allowlisted_kinds=%d actor_token_id_leaks=%d token_name_leaks=%d message_body_leaks=%d",
			report.NonAllowlistedKinds,
			report.ActorTokenIDLeaks,
			report.TokenNameLeaks,
			report.MessageBodyLeaks,
		)
	}
	report.Valid = true
	return report, nil
}

func privateStrings(ctx context.Context, pool *pgxpool.Pool, query string, args ...any) ([]string, error) {
	rows, err := pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func collectStrings(v any) []string {
	var out []string
	var walk func(any)
	walk = func(x any) {
		switch t := x.(type) {
		case map[string]any:
			for k, val := range t {
				out = append(out, k)
				walk(val)
			}
		case []any:
			for _, val := range t {
				walk(val)
			}
		case string:
			out = append(out, t)
		}
	}
	walk(v)
	return out
}

func leakedPrivateStringCount(exported, private []string) int {
	leaks := 0
	for _, p := range private {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if privateStringAppears(exported, p) {
			leaks++
		}
	}
	return leaks
}

func privateStringAppears(exported []string, private string) bool {
	for _, s := range exported {
		if s == private {
			return true
		}
		if len(private) >= 12 && strings.Contains(s, private) {
			return true
		}
	}
	return false
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
