// Package feed reads the chronological activity log out of `events` for
// human consumption.
//
// What appears in /v1/feed is a *policy* decision, not a domain truth: the
// feed is the user-visible narrative of what the system did, not the full
// audit log. Token issuance and idempotency replay caching are part of the
// audit log but not the narrative; work_item lifecycle changes and signal
// receptions are. The two classifications below codify that, and the test
// alongside this file forces every event kind in domain.AllEventKinds to be
// classified as one or the other (no silent drops when a new kind ships).
package feed

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/domain"
)

// IncludedKinds is the canonical allowlist of event kinds shown in
// /v1/feed. Treat as immutable; mutation is unsynchronized and there is
// no reason to mutate at runtime.
var IncludedKinds = []string{
	domain.EventMessageCaptured,
	domain.EventWorkItemCreated,
	domain.EventWorkItemTransitioned,
	domain.EventWorkItemEventAppended,
	domain.EventWorkItemRelationAdded,
	domain.EventWorkItemMetadataUpdated,
	domain.EventXylemExhausted,
	domain.EventSignalReceived,
	domain.EventDeterministicErrorReported,
	domain.EventDeterministicErrorMasked,
	domain.EventDeterministicErrorUnmasked,
	domain.EventEscalationRequested,
	domain.EventSubactorGrantRequested,
	domain.EventSubactorGrantGranted,
	domain.EventSubactorGrantDenied,
	domain.EventSubactorGrantEscalated,
	domain.EventCultivarActivationRequested,
	domain.EventCultivarActivationGranted,
	domain.EventCultivarActivationDenied,
	domain.EventCultivarActivationEscalated,
	domain.EventApprovalCreated,
	domain.EventApprovalDecided,
	domain.EventApprovalExpired,
	domain.EventHTTPConnectorActionRequested,
	domain.EventHTTPConnectorActionApproved,
	domain.EventHTTPConnectorActionSent,
	domain.EventPatienceBreached,
	domain.EventConvergenceVerdictRecorded,
	domain.EventConvergenceChecksProposed,
	domain.EventDispatchRequested,
	domain.EventPolicyProfileSwitched,
	domain.EventTropismDefined,
	domain.EventCultivarDefined,
	domain.EventProjectionDefined,
}

// ExcludedKinds enumerates the event kinds the system explicitly *does not*
// surface in /v1/feed. Membership here is a positive policy decision, not
// "we forgot": these kinds belong to the audit log (token administration,
// transport-layer replay caching) and would only add noise to the human
// activity narrative.
//
// Together, IncludedKinds and ExcludedKinds must partition
// domain.AllEventKinds. The test in feed_test.go enforces this; if a new
// event constant lands without being classified, that test fails and the
// contributor has to make the call.
var ExcludedKinds = []string{
	domain.EventTokenCreated,
	domain.EventTokenRevoked,
	domain.EventIdempotencyRecorded,
	// Assignment events carry actor identity and remain audit-only until
	// Assigned Lane defines their filtered/default-feed projection.
	domain.EventWorkItemAssigned,
	domain.EventWorkItemAssignmentReleased,
	// Node registry maintenance is fleet-topology audit, not the human
	// activity narrative; it belongs to the log, not /v1/feed.
	domain.EventNodeRegistered,
	domain.EventNodeRouteUpdated,
	domain.EventRegistrySnapshotObserved,
	// A queued cross-node command is transport plumbing: the durable parking
	// slot the target drains by outbound poll. The human activity narrative
	// surfaces the command's effect on its home node, not this hop.
	domain.EventCommandQueued,
	// A command ack is the matching drain-side plumbing: it records the
	// transport outcome onto the queue row, not human activity.
	domain.EventCommandAcked,
	// Attempts, terminal expiry, and poll-cursor advancement are transport
	// coordination facts. The human feed surfaces effects on home objects.
	domain.EventCommandAttempted,
	domain.EventCommandExpired,
	domain.EventCommandOutcomeObserved,
	domain.EventSpokeCursorAdvanced,
	// Provider OAuth client registration is auth-surface audit (RFC 7591
	// dynamic registration), not the human activity narrative; it belongs to
	// the log, not /v1/feed.
	domain.EventOAuthClientRegistered,
	domain.EventOAuthClientActorBound,
	domain.EventOAuthClientActorBindingRequested,
	domain.EventOAuthClientRevoked,
	domain.EventOAuthAuthorizationRequestCreated,
	domain.EventOAuthAuthorizationRequestCompleted,
	// Authorization code issue/redeem are OAuth transport plumbing: the
	// short-lived code exchanged for an access token. Auth-surface audit, not
	// human activity narrative.
	domain.EventOAuthAuthorizationCodeIssued,
	domain.EventOAuthAuthorizationCodeRedeemed,
	domain.EventOAuthGrantIssued,
	domain.EventOAuthGrantRefreshed,
	domain.EventOAuthGrantRevoked,
	domain.EventOAuthRefreshReuseDetected,
}

type Item struct {
	EventID      uuid.UUID       `json:"event_id"`
	OccurredAt   time.Time       `json:"occurred_at"`
	Source       domain.Source   `json:"source"`
	SubjectKind  string          `json:"subject_kind"`
	SubjectID    uuid.UUID       `json:"subject_id"`
	Kind         string          `json:"kind"`
	Payload      json.RawMessage `json:"payload"`
	ActorTokenID *uuid.UUID      `json:"actor_token_id,omitempty"`
	// Seq is the events.seq monotonic ordering primitive (added in
	// 70a2f732). Surfaced internally for cursor encoding; not exposed
	// in the JSON wire shape because cursor opacity forbids consumers
	// from reading sequence values directly.
	Seq int64 `json:"-"`
}

type Service struct {
	pool *pgxpool.Pool
}

func NewService(pool *pgxpool.Pool) *Service {
	return &Service{pool: pool}
}

const (
	defaultLimit = 50
	maxLimit     = 200
	maxWait      = 60 * time.Second
	pollTick     = 250 * time.Millisecond
)

// ListOptions selects between snapshot reads and watcher reads. When
// Cursor or Wait is set, the call enters watcher mode (oldest-first,
// after-cursor, optional bounded long-poll). When both are zero, callers
// should use List instead — the back-compat snapshot path.
type ListOptions struct {
	Cursor            string
	Wait              time.Duration
	Limit             int
	ProjectionName    string
	ProjectionVersion int
	Filter            *ProjectionFilter
	ReadFilter        *ReadFilter
}

// Page is the watcher-mode response. NextCursor MUST be round-tripped
// verbatim by the consumer; the encoding is a server-side implementation
// detail. An empty timeout preserves the caller cursor when nothing was
// scanned. If selected events were scanned but reduced away, NextCursor
// advances across them so the next call does not rescan invisible traffic.
type Page struct {
	Items      []Item `json:"items"`
	NextCursor string `json:"next_cursor"`
	HasMore    bool   `json:"has_more"`
	nextSeq    int64
}

// List returns the most recent feed-visible events, newest first. The kind
// filter is parameterized off IncludedKinds rather than inlined so the
// policy lives in one place (and a parity test can assert it stays in
// sync with the domain's known event kinds).
func (s *Service) List(ctx context.Context, limit int) ([]Item, error) {
	if limit <= 0 || limit > maxLimit {
		limit = defaultLimit
	}
	return s.legacyList(ctx, limit)
}

func (s *Service) ListFiltered(ctx context.Context, filter ProjectionFilter, limit int) ([]Item, error) {
	return s.ListWithReadFilter(ctx, ReadFilter{Projection: &filter}, limit)
}

// ListWithReadFilter applies the same normalized filter contract used by
// watcher and stream reads while preserving snapshot's newest-first order.
func (s *Service) ListWithReadFilter(ctx context.Context, filter ReadFilter, limit int) ([]Item, error) {
	if limit <= 0 || limit > maxLimit {
		limit = defaultLimit
	}
	normalized, err := NormalizeReadFilter(filter)
	if err != nil {
		return nil, err
	}
	return s.list(ctx, limit, normalized)
}

func (s *Service) list(ctx context.Context, limit int, filter ReadFilter) ([]Item, error) {
	kinds := filter.queryKinds()
	beforeSeq := int64(0)
	out := make([]Item, 0, limit)
	for len(out) < limit {
		rows, err := s.pool.Query(ctx, `
		SELECT id, occurred_at, actor_token_id, source, subject_kind, subject_id, kind, payload, seq
		FROM events
		WHERE kind = ANY($1::text[])
		AND ($2::bigint = 0 OR seq < $2)
		ORDER BY seq DESC
		LIMIT $3
	`, kinds, beforeSeq, maxLimit)
		if err != nil {
			return nil, err
		}
		batch := make([]Item, 0, maxLimit)
		for rows.Next() {
			item, err := scanItem(rows, true)
			if err != nil {
				rows.Close()
				return nil, err
			}
			beforeSeq = item.Seq
			batch = append(batch, item)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		rows.Close()
		matches, err := s.matchingItems(ctx, filter, batch)
		if err != nil {
			return nil, err
		}
		for i, item := range batch {
			if matches[i] {
				out = append(out, item)
				if len(out) == limit {
					break
				}
			}
		}
		if len(batch) < maxLimit {
			break
		}
	}
	return out, nil
}

func (s *Service) legacyList(ctx context.Context, limit int) ([]Item, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, occurred_at, actor_token_id, source, subject_kind, subject_id, kind, payload
		FROM events
		WHERE kind = ANY($1::text[])
		ORDER BY occurred_at DESC
		LIMIT $2
	`, IncludedKinds, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Item
	for rows.Next() {
		var item Item
		var source string
		if err := rows.Scan(&item.EventID, &item.OccurredAt, &item.ActorTokenID, &source, &item.SubjectKind, &item.SubjectID, &item.Kind, &item.Payload); err != nil {
			return nil, err
		}
		item.Source = domain.Source(source)
		out = append(out, item)
	}
	return out, rows.Err()
}

// Page returns events strictly after opts.Cursor in oldest-first order
// with at-least-once delivery — consumers MUST dedupe by EventID.
//
// When opts.Cursor is empty, the function snapshots the current head as
// the starting point ("from now" semantics): the first call to a fresh
// watcher returns nothing immediately and waits opts.Wait for new
// events; the consumer then resumes from NextCursor.
//
// When opts.Cursor is supplied, the seq it encodes is validated against
// the live events log (existence-check, repaired in 70a2f732 after B
// found that fabricated cursors were silently returning empty pages).
// Unknown seq → ErrInvalidCursor → 400 at the HTTP layer. The check
// happens before the after-cursor query so a fabricated future seq
// never gets dressed up as "no events after this point yet."
//
// When opts.Wait > 0 and the initial after-cursor query is empty, the
// call enters bounded poll mode: a 250ms tick that re-runs the query
// until events arrive, the wait cap fires, or the request context is
// cancelled. Pool conns are released between polls (no LISTEN/NOTIFY,
// no held connection) so 1k concurrent watchers don't starve the pool.
//
// Cursor opacity is contractual; consumers that parse the encoded blob
// will break at the first encoding change. See cursor.go for the v0
// (legacy) and v1 (current) shapes, both documented as server-internal.
func (s *Service) Page(ctx context.Context, opts ListOptions) (Page, error) {
	limit := opts.Limit
	if limit <= 0 || limit > maxLimit {
		limit = defaultLimit
	}
	wait := opts.Wait
	if wait < 0 {
		wait = 0
	}
	if wait > maxWait {
		wait = maxWait
	}

	projectionName := opts.ProjectionName
	projectionVersion := opts.ProjectionVersion
	var readFilter ReadFilter
	if opts.Filter != nil && projectionName != "" {
		readFilter.Projection = opts.Filter
	}
	if opts.ReadFilter != nil {
		readFilter = *opts.ReadFilter
	}
	var err error
	readFilter, err = NormalizeReadFilter(readFilter)
	if err != nil {
		return Page{}, err
	}

	filterFingerprint := readFilter.FingerprintHash()
	var cur cursor
	if opts.Cursor != "" {
		decoded, err := decodeCursorForIdentity(opts.Cursor, projectionName, projectionVersion, filterFingerprint)
		if err != nil {
			return Page{}, err
		}
		// Existence check: a syntactically-valid-but-fabricated cursor
		// (made-up seq, valid encoding) must 400 deterministically. seq=0
		// is a special case meaning "before any event" and is allowed
		// without a roundtrip — consumers building their own bootstrap
		// can encode seq=0 to mean "give me everything from the start."
		if decoded.seq > 0 {
			ok, err := s.cursorExists(ctx, decoded.seq)
			if err != nil {
				return Page{}, err
			}
			if !ok {
				return Page{}, fmt.Errorf("%w: seq %d was never issued", ErrInvalidCursor, decoded.seq)
			}
		}
		cur = decoded
	} else {
		head, err := s.head(ctx)
		if err != nil {
			return Page{}, err
		}
		cur = cursor{seq: head.seq, projection: projectionName, version: projectionVersion, filter: filterFingerprint}
	}

	deadline := time.Now().Add(wait)
	for {
		page, err := s.queryAfter(ctx, cur, limit, readFilter)
		if err != nil {
			return Page{}, err
		}
		if len(page.Items) > 0 {
			return page, nil
		}
		if page.nextSeq > cur.seq {
			cur.seq = page.nextSeq
		}
		if wait == 0 || !time.Now().Before(deadline) {
			return Page{
				Items:      []Item{},
				NextCursor: encodeCursorFor(cur.seq, projectionName, projectionVersion, filterFingerprint),
				HasMore:    false,
				nextSeq:    cur.seq,
			}, nil
		}
		select {
		case <-ctx.Done():
			return Page{}, ctx.Err()
		case <-time.After(pollTick):
		}
	}
}

// head returns a cursor pointing at the highest-seq event in the events
// log (across all kinds, not just feed-visible ones — the cursor is a
// pointer into the underlying log, not the projection). Used to
// bootstrap "from now" watcher mode when no caller cursor is supplied.
//
// When the log is empty, returns seq=0 — which encodeCursor renders as
// the all-zeros cursor and decodeCursor accepts as "before any event."
// Consumers that bootstrap against an empty log get a cursor that, on
// the next call, returns events 1..N (everything written since).
func (s *Service) head(ctx context.Context) (cursor, error) {
	row := s.pool.QueryRow(ctx, `SELECT COALESCE(MAX(seq), 0) FROM events`)
	var seq int64
	if err := row.Scan(&seq); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return cursor{seq: 0}, nil
		}
		return cursor{}, err
	}
	return cursor{seq: seq}, nil
}

// cursorExists verifies that the seq encoded in a consumer-supplied
// cursor was actually emitted by this server. Closes the contract gap
// B found in e1625848: without this check, a fabricated cursor decodes
// cleanly and the after-cursor query returns "events after that point"
// — empty for a future seq, masking consumer bugs.
//
// The check runs against ANY event (not just feed-visible), because the
// cursor IS into the underlying log. A consumer that received a cursor
// pointing at an idempotency.recorded event (excluded from /v1/feed)
// must still be able to round-trip it; the after-cursor query will skip
// non-feed-visible events, but the cursor itself is valid.
func (s *Service) cursorExists(ctx context.Context, seq int64) (bool, error) {
	var found bool
	row := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM events WHERE seq = $1)`, seq)
	if err := row.Scan(&found); err != nil {
		return false, err
	}
	return found, nil
}

// EncodeCursor turns a seq into the v1 opaque cursor wire string. Public
// because the SSE handler needs to stamp `id:` frames with the same value
// browsers will echo back as Last-Event-ID on reconnect.
func EncodeCursor(seq int64) string {
	return encodeCursor(seq)
}

// EncodeCursorForProjection returns the opaque cursor shape for a named feed
// projection. It is public for the SSE handler, which stamps event ids outside
// the feed package.
func EncodeCursorForProjection(seq int64, projection string, version int) string {
	return encodeCursorFor(seq, projection, version, "")
}

// EncodeCursorForIdentity stamps the full channel identity — projection plus
// canonical predicate fingerprint — into the cursor. The SSE handler uses it
// for `id:` frames on filtered streams so Last-Event-ID resumes carry the
// identity they were issued under.
func EncodeCursorForIdentity(seq int64, projection string, version int, filter string) string {
	return encodeCursorFor(seq, projection, version, filter)
}

// ResolveStreamStart decodes cursorStr into the seq the SSE handler will
// use as its "strictly after" point. Empty cursorStr means "from now":
// the function returns the current MAX(seq) so the stream sees only
// events appended after the connection opens. A supplied cursorStr is
// decoded and existence-checked exactly as Page() does — same 400 path
// for fabricated cursors, same v0-format invalidation for old clients.
//
// This is the SSE counterpart to head() + cursorExists() inside Page; it
// exists as a separate entry point because the SSE handler doesn't want
// the long-poll loop, just the start position.
func (s *Service) ResolveStreamStart(ctx context.Context, cursorStr string) (int64, error) {
	return s.ResolveStreamStartForProjection(ctx, cursorStr, "", 0)
}

func (s *Service) ResolveStreamStartForProjection(ctx context.Context, cursorStr string, projectionName string, projectionVersion int) (int64, error) {
	return s.ResolveStreamStartForIdentity(ctx, cursorStr, projectionName, projectionVersion, "")
}

// ResolveStreamStartForIdentity is ResolveStreamStartForProjection with the
// canonical predicate fingerprint included in the identity check.
func (s *Service) ResolveStreamStartForIdentity(ctx context.Context, cursorStr string, projectionName string, projectionVersion int, filterFingerprint string) (int64, error) {
	if cursorStr == "" {
		head, err := s.head(ctx)
		if err != nil {
			return 0, err
		}
		return head.seq, nil
	}
	decoded, err := decodeCursorForIdentity(cursorStr, projectionName, projectionVersion, filterFingerprint)
	if err != nil {
		return 0, err
	}
	if decoded.seq > 0 {
		ok, err := s.cursorExists(ctx, decoded.seq)
		if err != nil {
			return 0, err
		}
		if !ok {
			return 0, fmt.Errorf("%w: seq %d was never issued", ErrInvalidCursor, decoded.seq)
		}
	}
	return decoded.seq, nil
}

// Tail returns at most limit feed-visible events with seq > fromSeq,
// oldest-first, in a single non-blocking query. The SSE handler calls
// this in a tight poll loop between heartbeats; pool conns are released
// between calls so 100 concurrent streams don't pin the pool.
//
// Item.Seq is populated; the SSE handler stamps it onto the SSE id frame
// via EncodeCursor.
func (s *Service) Tail(ctx context.Context, fromSeq int64, limit int) ([]Item, error) {
	return s.TailFiltered(ctx, fromSeq, limit, nil)
}

func (s *Service) TailFiltered(ctx context.Context, fromSeq int64, limit int, filter *ProjectionFilter) ([]Item, error) {
	readFilter := ReadFilter{Projection: filter}
	batch, err := s.TailWithReadFilter(ctx, fromSeq, limit, readFilter)
	if err != nil {
		return nil, err
	}
	return batch.Items, nil
}

// TailBatch separates emitted items from the highest event sequence examined.
// SSE advances ScannedThrough even when every candidate was reduced away,
// preventing an invisible event from being rescanned forever.
type TailBatch struct {
	Items          []Item
	ScannedThrough int64
}

func (s *Service) TailWithReadFilter(ctx context.Context, fromSeq int64, limit int, filter ReadFilter) (TailBatch, error) {
	if limit <= 0 || limit > maxLimit {
		limit = defaultLimit
	}
	normalized, err := NormalizeReadFilter(filter)
	if err != nil {
		return TailBatch{}, err
	}
	page, err := s.queryAfter(ctx, cursor{seq: fromSeq}, limit, normalized)
	if err != nil {
		return TailBatch{}, err
	}
	return TailBatch{Items: page.Items, ScannedThrough: page.nextSeq}, nil
}

// queryAfter runs a single after-cursor SELECT against the seq column.
// seq is BIGSERIAL (strictly increasing per insert under default
// isolation), so WHERE seq > $cursor ORDER BY seq is a true monotonic
// resume primitive — no skip risk under same-microsecond contention,
// no compound-key tuple comparison, no dependency on content-addressed
// id ordering.
//
// limit+1 is fetched to compute HasMore without a separate COUNT query.
func (s *Service) queryAfter(ctx context.Context, cur cursor, limit int, filter ReadFilter) (Page, error) {
	kinds := filter.queryKinds()
	batchLimit := maxLimit + 1
	items := make([]Item, 0, limit+1)
	fromSeq := cur.seq
	scannedSeq := cur.seq
	for len(items) <= limit {
		rows, err := s.pool.Query(ctx, `
		SELECT id, occurred_at, actor_token_id, source, subject_kind, subject_id, kind, payload, seq
		FROM events
		WHERE kind = ANY($1::text[])
		AND seq > $2
		ORDER BY seq ASC
		LIMIT $3
	`, kinds, fromSeq, batchLimit)
		if err != nil {
			return Page{}, err
		}
		batch := make([]Item, 0, batchLimit)
		for rows.Next() {
			item, err := scanItem(rows, true)
			if err != nil {
				rows.Close()
				return Page{}, err
			}
			scannedSeq = item.Seq
			batch = append(batch, item)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return Page{}, err
		}
		rows.Close()
		matches, err := s.matchingItems(ctx, filter, batch)
		if err != nil {
			return Page{}, err
		}
		for i, item := range batch {
			if matches[i] {
				items = append(items, item)
				if len(items) > limit {
					break
				}
			}
		}
		if len(items) > limit || len(batch) < batchLimit {
			break
		}
		fromSeq = scannedSeq
	}

	hasMore := len(items) > limit
	if hasMore {
		items = items[:limit]
	}
	nextSeq := scannedSeq
	if len(items) == 0 {
		return Page{
			Items:      []Item{},
			NextCursor: encodeCursorFor(nextSeq, cur.projection, cur.version, cur.filter),
			HasMore:    false,
			nextSeq:    nextSeq,
		}, nil
	}
	last := items[len(items)-1]
	if hasMore {
		nextSeq = last.Seq
	}
	return Page{
		Items:      items,
		NextCursor: encodeCursorFor(nextSeq, cur.projection, cur.version, cur.filter),
		HasMore:    hasMore,
		nextSeq:    nextSeq,
	}, nil
}

type itemScanner interface {
	Scan(dest ...any) error
}

func scanItem(row itemScanner, includeSeq bool) (Item, error) {
	var item Item
	var source string
	if includeSeq {
		if err := row.Scan(&item.EventID, &item.OccurredAt, &item.ActorTokenID, &source, &item.SubjectKind, &item.SubjectID, &item.Kind, &item.Payload, &item.Seq); err != nil {
			return Item{}, err
		}
	} else if err := row.Scan(&item.EventID, &item.OccurredAt, &item.ActorTokenID, &source, &item.SubjectKind, &item.SubjectID, &item.Kind, &item.Payload); err != nil {
		return Item{}, err
	}
	item.Source = domain.Source(source)
	return item, nil
}

func encodeCursorFor(seq int64, projection string, version int, filter string) string {
	if projection == "" && filter == "" {
		return encodeCursor(seq)
	}
	return encodeIdentityCursor(seq, projection, version, filter)
}
