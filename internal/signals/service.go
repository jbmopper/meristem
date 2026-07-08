package signals

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/idempotency"
	"github.com/jbmopper/meristem/internal/policyprofile"
	"github.com/jbmopper/meristem/internal/safety"
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

	// ErrSignalItemBudgetExhausted is returned after Receive has committed the
	// signal row for audit, recorded a structured xylem.exhausted event, and
	// raised the owner escalation — but refused to create the NEW work_item the
	// signal asked for because the source token is over its per-hour admission
	// budget (safety.Policy.MaxSignalItemsPerTokenPerHour). Dedupe-linked
	// signals never reach this path; only item creation is metered. The HTTP
	// handler maps this to a structured 409 refusal.
	ErrSignalItemBudgetExhausted = errors.New("signals: source-token signal admission budget exhausted")
)

// Service is the signal-reception domain method type. It is the only thing
// that knows how a signal becomes a work_item: the API handler is a thin
// caller (see docs/coord/2026-04-23-parallel-work.md "/v1/signals
// ownership"). All side effects are events.Writer.Append calls inside a
// single transaction the service owns.
type Service struct {
	pool     *pgxpool.Pool
	writer   *events.Writer
	profiles *policyprofile.Service
}

// NewService wires a Service to its pool and the shared events.Writer (the
// one with every projector registered, i.e. the one app.NewEventWriter
// returns). Using a writer with a partial registry will silently miss
// projection writes; callers should not do that.
func NewService(pool *pgxpool.Pool, writer *events.Writer) *Service {
	return &Service{pool: pool, writer: writer, profiles: policyprofile.NewService(pool, writer)}
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

	// Resolve the active admission budget before opening the transaction. The
	// active-profile read borrows its own pool connection; doing it here keeps
	// it from nesting a second connection acquisition inside the transaction,
	// which would deadlock a small pool under concurrent receivers that already
	// hold a connection while blocked on the dedupe advisory lock.
	maxItems, err := s.signalItemBudget(ctx)
	if err != nil {
		return ReceiveResult{}, err
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return ReceiveResult{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := lockDedupeKey(ctx, tx, in.DedupeKey); err != nil {
		return ReceiveResult{}, err
	}

	workItemID, linked, err := s.lookupLiveWorkItem(ctx, tx, in.DedupeKey)
	if err != nil {
		return ReceiveResult{}, err
	}

	var workItemEventID uuid.UUID
	created := false
	admitted := true
	var budgetErr error
	if !linked {
		// Only NEW work_item creation is metered. Dedupe-linked signals never
		// reach this branch, so attaching to an existing live item stays
		// unmetered by construction.
		current, err := countCreatingSignalsLastHour(ctx, tx, actor.ID)
		if err != nil {
			return ReceiveResult{}, err
		}
		if admitItemCreation(current, maxItems) {
			workItemID = newSubjectID(ctx, "signals.work_item")
			workItemEventID, _, err = s.writer.Append(ctx, tx, events.Spec{
				SubjectKind:  domain.SubjectWorkItem,
				SubjectID:    workItemID,
				Kind:         domain.EventWorkItemCreated,
				Source:       actorSource,
				ActorTokenID: &actor.ID,
				Payload: map[string]any{
					"title":                        strings.TrimSpace(header.Title),
					"body":                         workItemBodyFrom(header),
					"state":                        domain.WorkItemCaptured,
					"suggested_convergence_checks": header.AcceptanceCriteria,
					"human_review_status":          domain.HumanReviewWavedThrough,
				},
			})
			if err != nil {
				return ReceiveResult{}, fmt.Errorf("signals: append work_item.created: %w", err)
			}
			created = true
		} else {
			// Over budget: refuse the creation but still record the signal for
			// audit (below), mark the exhaustion, and escalate to the owner.
			admitted = false
			workItemID = uuid.Nil
			if err := s.recordSignalBudgetExhausted(ctx, tx, actor, signalID, current, maxItems); err != nil {
				return ReceiveResult{}, err
			}
			budgetErr = fmt.Errorf(
				"%w: source token %s exceeded the signal admission budget (created_last_hour=%d max=%d window=1h); signal %s recorded for audit, work_item creation refused",
				ErrSignalItemBudgetExhausted, actor.ID, current, maxItems, signalID,
			)
		}
	}

	payload := map[string]any{
		"signal_kind":       in.SignalKind,
		"fingerprint":       fingerprint,
		"work_spec":         json.RawMessage(in.WorkSpec),
		"created_work_item": created,
	}
	if admitted {
		payload["work_item_id"] = workItemID
	} else {
		// No work_item was created; the projection row records a NULL
		// work_item_id and this flag so audits can tell a budget refusal apart
		// from an ordinary dedupe link.
		payload["item_creation_admitted"] = false
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

	result := ReceiveResult{
		SignalID:        signalID,
		SignalEventID:   signalEventID,
		WorkItemID:      workItemID,
		WorkItemEventID: workItemEventID,
		CreatedWorkItem: created,
		Fingerprint:     fingerprint,
		DedupeKey:       in.DedupeKey,
		SignalKind:      in.SignalKind,
	}
	// The signal row and escalation are committed above; the refusal is
	// surfaced to the transport as a structured error while the audit trail
	// persists.
	return result, budgetErr
}

// signalItemBudget resolves the active profile's per-token hourly item-creation
// budget. Resolving through the active profile (rather than a hard-coded
// default) is what makes the tighter bring-up budget real when the operator has
// switched posture; absent any switch it falls back to steady exactly like
// every other active-profile read.
func (s *Service) signalItemBudget(ctx context.Context) (int, error) {
	if s.profiles == nil {
		return safety.DefaultPolicy().MaxSignalItemsPerTokenPerHour, nil
	}
	active, err := s.profiles.Active(ctx)
	if err != nil {
		return 0, fmt.Errorf("signals: resolve active signal budget: %w", err)
	}
	return active.Policy.MaxSignalItemsPerTokenPerHour, nil
}

// admitItemCreation is the pure admission decision: a new work_item is admitted
// only while the count already created by this token in the rolling hour is
// strictly below the budget. Mirrors the "current < budget.Max" idiom the
// work_item xylem budgets use.
func admitItemCreation(createdThisHour, max int) bool {
	return createdThisHour < max
}

// countCreatingSignalsLastHour counts the work_item-creating signals recorded
// for a token within the trailing hour, straight from the signals projection —
// no in-process counter, so the budget survives restarts (AGENTS.md: process
// state belongs in Postgres). Dedupe-linked and previously-refused signals do
// not carry created_work_item = true, so they never inflate the count.
func countCreatingSignalsLastHour(ctx context.Context, tx pgx.Tx, actorTokenID uuid.UUID) (int, error) {
	var count int
	err := tx.QueryRow(ctx, `
		SELECT count(*)
		FROM signals
		WHERE actor_token_id = $1
		  AND created_work_item = true
		  AND received_at >= now() - interval '1 hour'
	`, actorTokenID).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("signals: count creating signals for budget: %w", err)
	}
	return count, nil
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
// Concurrent receivers with the same dedupe_key are serialized by
// lockDedupeKey before this lookup runs. That keeps the "one live work_item
// per dedupe_key" contract true even when callers use distinct idempotency
// keys, while preserving recurrence semantics once the prior item is
// terminal.
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

func lockDedupeKey(ctx context.Context, tx pgx.Tx, dedupeKey string) error {
	if dedupeKey == "" {
		return nil
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, dedupeLockKeyFor(dedupeKey)); err != nil {
		return fmt.Errorf("signals: dedupe lock: %w", err)
	}
	return nil
}

func dedupeLockKeyFor(dedupeKey string) int64 {
	sum := sha256.Sum256([]byte("signals.dedupe|" + dedupeKey))
	return int64(binary.BigEndian.Uint64(sum[:8]))
}

// workSpecHeader is the small slice of work_spec the service reads to
// populate the work_item.created event. Full schema validation (including
// accepted legacy schema_version strings in internal/api) is the handler's
// responsibility; the service only needs enough to write the projection row honestly.
type workSpecHeader struct {
	Title              string   `json:"title"`
	Objective          string   `json:"objective"`
	Details            string   `json:"details"`
	AcceptanceCriteria []string `json:"acceptance_criteria"`
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

// recordSignalBudgetExhausted records the deterministic xylem.exhausted event
// for a refused signal (subject: the signal itself) and raises the owner
// escalation. Both run inside the caller's transaction so they commit together
// with the audited signal row.
func (s *Service) recordSignalBudgetExhausted(ctx context.Context, tx pgx.Tx, actor domain.Token, signalID uuid.UUID, current, max int) error {
	source := sourceForActor(actor)
	if _, _, err := s.writer.Append(ctx, tx, events.Spec{
		SubjectKind:  domain.SubjectSignal,
		SubjectID:    signalID,
		Kind:         domain.EventXylemExhausted,
		Source:       source,
		ActorTokenID: &actor.ID,
		Payload: map[string]any{
			"budget":                              "max_signal_items_per_token_per_hour",
			"current_signal_items":                current,
			"max_signal_items_per_token_per_hour": max,
			"window_seconds":                      3600,
			"budget_source":                       "safety_policy",
			"escalation_rule":                     string(domain.EscalationRuleHandToHuman),
			"actor_token_id":                      actor.ID,
			"refused_signal_id":                   signalID,
			"count_scope":                         "same_source_token_created_work_items_last_hour",
		},
	}); err != nil {
		return fmt.Errorf("signals: append xylem.exhausted: %w", err)
	}
	return s.raiseSignalBudgetEscalation(ctx, tx, actor, current, max)
}

// raiseSignalBudgetEscalation raises a self-contained human-attention
// escalation for a token that is over its signal admission budget. There is no
// offending work_item to block (the whole point is that none was created), so
// this appends escalation.requested plus a fresh human-attention work_item
// rather than routing through escalations.Service (which escalates an existing
// item). The escalation identity is keyed on (token, hour bucket) so a hostile
// reporter emitting thousands of over-budget signals produces at most one
// escalation per hour — bounding the very work-item growth this budget exists
// to stop — and retries within the window collapse onto the same escalation.
func (s *Service) raiseSignalBudgetEscalation(ctx context.Context, tx pgx.Tx, actor domain.Token, current, max int) error {
	bucket := time.Now().UTC().Truncate(time.Hour).Format(time.RFC3339)
	escalationID := signalBudgetEscalationID(actor.ID, bucket)
	humanWorkItemID := signalBudgetHumanWorkItemID(escalationID)
	if ok, err := signalEscalationExists(ctx, tx, escalationID); err != nil {
		return err
	} else if ok {
		return nil
	}
	source := sourceForActor(actor)
	reason := "signal admission budget exhausted: max_signal_items_per_token_per_hour"
	summary := fmt.Sprintf(
		"Source token %s exceeded its signal admission budget: created_work_items_last_hour=%d max=%d window_seconds=3600. New work_item creation from this token's signals is being refused; recorded signals remain in the audit log.",
		actor.ID, current, max,
	)
	if _, _, err := s.writer.Append(ctx, tx, events.Spec{
		SubjectKind:  domain.SubjectEscalation,
		SubjectID:    escalationID,
		Kind:         domain.EventEscalationRequested,
		Source:       source,
		ActorTokenID: &actor.ID,
		Payload: map[string]any{
			"human_work_item_id":                  humanWorkItemID,
			"reason":                              reason,
			"summary":                             summary,
			"source_token_id":                     actor.ID,
			"budget":                              "max_signal_items_per_token_per_hour",
			"current_signal_items":                current,
			"max_signal_items_per_token_per_hour": max,
			"window_seconds":                      3600,
			"hour_bucket":                         bucket,
		},
	}); err != nil {
		return fmt.Errorf("signals: append escalation.requested: %w", err)
	}
	if _, _, err := s.writer.Append(ctx, tx, events.Spec{
		SubjectKind:  domain.SubjectWorkItem,
		SubjectID:    humanWorkItemID,
		Kind:         domain.EventWorkItemCreated,
		Source:       source,
		ActorTokenID: &actor.ID,
		Payload: map[string]any{
			"title":                        "Human attention: signal admission budget exhausted",
			"body":                         signalBudgetHumanWorkItemBody(actor.ID, reason, summary),
			"state":                        domain.WorkItemCaptured,
			"suggested_convergence_checks": []string{"human_response_recorded"},
			"human_review_status":          domain.HumanReviewBlocked,
		},
	}); err != nil {
		return fmt.Errorf("signals: append escalation human work_item.created: %w", err)
	}
	return nil
}

func signalEscalationExists(ctx context.Context, tx pgx.Tx, escalationID uuid.UUID) (bool, error) {
	var ok bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM events
			WHERE subject_kind = $1 AND subject_id = $2 AND kind = $3
		)
	`, domain.SubjectEscalation, escalationID, domain.EventEscalationRequested).Scan(&ok); err != nil {
		return false, fmt.Errorf("signals: check existing escalation: %w", err)
	}
	return ok, nil
}

func signalBudgetEscalationID(actorTokenID uuid.UUID, hourBucket string) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(strings.Join([]string{
		"meristem",
		"escalation",
		"signal-admission-budget",
		actorTokenID.String(),
		hourBucket,
	}, "\x00")))
}

func signalBudgetHumanWorkItemID(escalationID uuid.UUID) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(strings.Join([]string{
		"meristem",
		"escalation",
		"human-work-item",
		escalationID.String(),
	}, "\x00")))
}

func signalBudgetHumanWorkItemBody(actorTokenID uuid.UUID, reason, summary string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Escalation requested for source token %s.\n\n", actorTokenID)
	fmt.Fprintf(&b, "Reason: %s\n\n", reason)
	fmt.Fprintf(&b, "Summary: %s\n\n", summary)
	fmt.Fprintf(&b, "Investigate the reporter behind this token (a bug rotating dedupe_keys, or abuse). Signals are still recorded for audit; work_item creation from this token resumes automatically once its trailing-hour creation count falls back below budget.")
	return b.String()
}
