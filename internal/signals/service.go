package signals

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/idempotency"
)

// Sentinel errors. The HTTP handler maps these to 400; everything else is
// treated as 500 (or whatever the underlying error suggests). Keep the set
// small and documented in docs/signals.md "Status codes" so contract drift
// shows up in review.
var (
	ErrSignalKindRequired   = errors.New("signals: signal_kind is required")
	ErrWorkSpecRequired     = errors.New("signals: work_spec is required")
	ErrWorkSpecInvalid      = errors.New("signals: work_spec must be valid JSON")
	ErrWorkSpecMissingTitle = errors.New("signals: work_spec.title is required")
)

// Service is the signal-reception domain method type. It is the only thing
// that knows how a signal becomes a work_item: the API handler is a thin
// caller (see docs/coord/2026-04-23-parallel-work.md "/v1/signals
// ownership"). All side effects are events.Writer.Append calls inside a
// single transaction the service owns.
type Service struct {
	pool   *pgxpool.Pool
	writer *events.Writer
}

// NewService wires a Service to its pool and the shared events.Writer (the
// one with every projector registered, i.e. the one app.NewEventWriter
// returns). Using a writer with a partial registry will silently miss
// projection writes; callers should not do that.
func NewService(pool *pgxpool.Pool, writer *events.Writer) *Service {
	return &Service{pool: pool, writer: writer}
}

// SourceMetadata mirrors the optional body.source field on POST /v1/signals.
// It is *content* per AGENTS.md principle 5 — the authoritative
// (events.source, events.actor_token_id) attribution comes from the bearer
// token, not from this. Receive preserves SourceMetadata in the
// signal.received event payload so audits can reconstruct provenance; v0
// does not project it into a column on the signals table (see
// docs/signals.md "Projection").
type SourceMetadata struct {
	Kind        string
	Identifier  string
	ExternalRef string
}

func (m SourceMetadata) empty() bool {
	return m.Kind == "" && m.Identifier == "" && m.ExternalRef == ""
}

// ReceiveInput is the parsed request the handler hands to Receive.
//
// Schema validation against docs/schemas/meristem.work_spec.v1.json is the
// handler's responsibility — Receive only enforces what it cannot do its
// own work without (a non-empty signal_kind, a JSON-valid work_spec with a
// title to put on the work_items row).
type ReceiveInput struct {
	SignalKind string
	DedupeKey  string
	Source     SourceMetadata
	WorkSpec   json.RawMessage
}

// ReceiveResult exposes the four identities the response envelope renders
// (see docs/signals.md "Endpoint" → response example). The handler is also
// responsible for the Idempotency-Key value; the service only needs the
// idempotency context for deterministic subject ids.
//
// WorkItemEventID is uuid.Nil when no work_item.created event was appended
// during this call (i.e. the dedupe lookup linked to a live work_item).
// CreatedWorkItem is the same signal as a boolean.
type ReceiveResult struct {
	SignalID        uuid.UUID
	SignalEventID   uuid.UUID
	WorkItemID      uuid.UUID
	WorkItemEventID uuid.UUID
	CreatedWorkItem bool
	Fingerprint     string
	DedupeKey       string
	SignalKind      string
}

// Receive performs the full signal reception in one transaction:
//
//  1. Resolve work_item_id by dedupe lookup against *live* work_items per
//     docs/coord/2026-04-23-parallel-work.md "Signal dedupe semantics".
//  2. If no live match (or no dedupe_key), append work_item.created with a
//     stable id derived from the idempotency context.
//  3. Always append signal.received with that resolved work_item_id.
//  4. Commit.
//
// All ids are derived from the idempotency context when present so that
// retries past the response cache horizon converge on the same durable
// events. Without idempotency context (tests, programmatic callers),
// Receive falls back to uuid.New so each call is a distinct signal.
func (s *Service) Receive(ctx context.Context, actor domain.Token, in ReceiveInput) (ReceiveResult, error) {
	in.SignalKind = strings.TrimSpace(in.SignalKind)
	in.DedupeKey = strings.TrimSpace(in.DedupeKey)
	if in.SignalKind == "" {
		return ReceiveResult{}, ErrSignalKindRequired
	}
	if len(bytes.TrimSpace(in.WorkSpec)) == 0 {
		return ReceiveResult{}, ErrWorkSpecRequired
	}
	if !json.Valid(in.WorkSpec) {
		return ReceiveResult{}, ErrWorkSpecInvalid
	}

	header, err := decodeWorkSpecHeader(in.WorkSpec)
	if err != nil {
		return ReceiveResult{}, err
	}
	if strings.TrimSpace(header.Title) == "" {
		return ReceiveResult{}, ErrWorkSpecMissingTitle
	}

	fingerprint, err := computeFingerprint(in.WorkSpec)
	if err != nil {
		return ReceiveResult{}, fmt.Errorf("signals: fingerprint: %w", err)
	}

	signalID := newSubjectID(ctx, "signals.signal")
	actorSource := sourceForActor(actor)

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return ReceiveResult{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	workItemID, linked, err := s.lookupLiveWorkItem(ctx, tx, in.DedupeKey)
	if err != nil {
		return ReceiveResult{}, err
	}

	var workItemEventID uuid.UUID
	created := false
	if !linked {
		workItemID = newSubjectID(ctx, "signals.work_item")
		workItemEventID, _, err = s.writer.Append(ctx, tx, events.Spec{
			SubjectKind:  domain.SubjectWorkItem,
			SubjectID:    workItemID,
			Kind:         domain.EventWorkItemCreated,
			Source:       actorSource,
			ActorTokenID: &actor.ID,
			Payload: map[string]any{
				"title": strings.TrimSpace(header.Title),
				"body":  workItemBodyFrom(header),
				"state": domain.WorkItemCaptured,
			},
		})
		if err != nil {
			return ReceiveResult{}, fmt.Errorf("signals: append work_item.created: %w", err)
		}
		created = true
	}

	payload := map[string]any{
		"signal_kind":       in.SignalKind,
		"fingerprint":       fingerprint,
		"work_spec":         json.RawMessage(in.WorkSpec),
		"work_item_id":      workItemID,
		"created_work_item": created,
	}
	if in.DedupeKey != "" {
		payload["dedupe_key"] = in.DedupeKey
	}
	if !in.Source.empty() {
		payload["source_metadata"] = sourceMetadataPayload(in.Source)
	}

	signalEventID, _, err := s.writer.Append(ctx, tx, events.Spec{
		SubjectKind:  domain.SubjectSignal,
		SubjectID:    signalID,
		Kind:         domain.EventSignalReceived,
		Source:       actorSource,
		ActorTokenID: &actor.ID,
		Payload:      payload,
	})
	if err != nil {
		return ReceiveResult{}, fmt.Errorf("signals: append signal.received: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return ReceiveResult{}, err
	}

	return ReceiveResult{
		SignalID:        signalID,
		SignalEventID:   signalEventID,
		WorkItemID:      workItemID,
		WorkItemEventID: workItemEventID,
		CreatedWorkItem: created,
		Fingerprint:     fingerprint,
		DedupeKey:       in.DedupeKey,
		SignalKind:      in.SignalKind,
	}, nil
}

// lookupLiveWorkItem implements the dedupe semantics confirmed in
// docs/coord/2026-04-23-parallel-work.md "Signal dedupe semantics":
// only currently-live work_items satisfy a dedupe match. A terminal prior
// work_item (done/failed/canceled) means the new signal is a recurrence
// and the caller should create a fresh work_item.
//
// Returns (id, true, nil) on a live match; (uuid.Nil, false, nil) when
// nothing matches OR when dedupeKey is empty (every signal without a
// dedupe_key is treated as fresh).
//
// Concurrency note: two concurrent receivers with the same dedupe_key but
// different idempotency keys can both observe "no live work_item" and
// create one each. The next reception will dedupe to the most recent. Full
// reservation/locking is on Agent B's substrate to-do list (see coord doc).
func (s *Service) lookupLiveWorkItem(ctx context.Context, tx pgx.Tx, dedupeKey string) (uuid.UUID, bool, error) {
	if dedupeKey == "" {
		return uuid.Nil, false, nil
	}
	var workItemID uuid.UUID
	err := tx.QueryRow(ctx, `
		SELECT s.work_item_id
		FROM signals s
		JOIN work_items w ON w.id = s.work_item_id
		WHERE s.dedupe_key = $1
		  AND w.state NOT IN ('done', 'failed', 'canceled')
		ORDER BY s.received_at DESC
		LIMIT 1
	`, dedupeKey).Scan(&workItemID)
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, false, nil
	}
	if err != nil {
		return uuid.Nil, false, fmt.Errorf("signals: dedupe lookup: %w", err)
	}
	return workItemID, true, nil
}

// workSpecHeader is the small slice of work_spec the service reads to
// populate the work_item.created event. Full schema validation against
// meristem.work_spec.v1 (and legacy maristem.work_spec.v1, legacy.work_spec.v1) is the handler's
// responsibility; the service
// only needs enough to write the projection row honestly.
type workSpecHeader struct {
	Title     string `json:"title"`
	Objective string `json:"objective"`
	Details   string `json:"details"`
}

func decodeWorkSpecHeader(raw json.RawMessage) (workSpecHeader, error) {
	var h workSpecHeader
	if err := json.Unmarshal(raw, &h); err != nil {
		return workSpecHeader{}, fmt.Errorf("%w: %v", ErrWorkSpecInvalid, err)
	}
	return h, nil
}

// workItemBodyFrom picks the human-readable summary that goes into
// work_items.body when a signal creates a work_item. Objective is the
// schema's "what done looks like" field; details is the diagnosis. Either
// is acceptable; "" is acceptable too (the projector tolerates empty body).
func workItemBodyFrom(h workSpecHeader) string {
	if strings.TrimSpace(h.Objective) != "" {
		return h.Objective
	}
	return h.Details
}

// computeFingerprint canonicalizes work_spec and returns the hex-encoded
// SHA-256 of the canonical bytes. Two callers sending byte-different but
// semantically-identical work_specs (e.g. different key ordering) produce
// the same fingerprint — that's the entire point of the "content identity"
// row in docs/signals.md "The four identities".
func computeFingerprint(raw json.RawMessage) (string, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return "", err
	}
	canonical, err := events.CanonicalJSON(v)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), nil
}

func sourceMetadataPayload(m SourceMetadata) map[string]any {
	out := map[string]any{}
	if m.Kind != "" {
		out["kind"] = m.Kind
	}
	if m.Identifier != "" {
		out["identifier"] = m.Identifier
	}
	if m.ExternalRef != "" {
		out["external_ref"] = m.ExternalRef
	}
	return out
}

func sourceForActor(actor domain.Token) domain.Source {
	if actor.Source.Valid() {
		return actor.Source
	}
	return domain.SourceHuman
}

func newSubjectID(ctx context.Context, label string) uuid.UUID {
	if id, ok := idempotency.SubjectID(ctx, label); ok {
		return id
	}
	return uuid.New()
}
