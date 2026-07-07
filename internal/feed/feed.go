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
	// Node registry maintenance is fleet-topology audit, not the human
	// activity narrative; it belongs to the log, not /v1/feed.
	domain.EventNodeRegistered,
	domain.EventNodeRouteUpdated,
	// A queued cross-node command is transport plumbing: the durable parking
	// slot the target drains by outbound poll. The human activity narrative
	// surfaces the command's effect on its home node, not this hop.
	domain.EventCommandQueued,
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
}

// Page is the watcher-mode response. NextCursor MUST be round-tripped
// verbatim by the consumer; the encoding is a server-side implementation
// detail. When the page has no items but the wait timed out, NextCursor
// is the same cursor the caller sent (so the next call resumes from the
// same point).
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
	if limit <= 0 || limit > maxLimit {
		limit = defaultLimit
	}
	return s.list(ctx, limit, &filter)
}

func (s *Service) list(ctx context.Context, limit int, filter *ProjectionFilter) ([]Item, error) {
	kinds := IncludedKinds
	if filter != nil {
		kinds = filter.QueryKinds()
	}
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
		scanned := 0
		for rows.Next() {
			scanned++
			item, err := scanItem(rows, true)
			if err != nil {
				rows.Close()
				return nil, err
			}
			beforeSeq = item.Seq
			if filter == nil || filter.Matches(item) {
				out = append(out, item)
				if len(out) == limit {
					break
				}
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		rows.Close()
		if scanned < maxLimit {
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
	var filter *ProjectionFilter
	if opts.Filter != nil && projectionName != "" {
		filter = opts.Filter
	}

	var cur cursor
	if opts.Cursor != "" {
		decoded, err := decodeCursorForProjection(opts.Cursor, projectionName, projectionVersion)
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
		cur = cursor{seq: head.seq, projection: projectionName, version: projectionVersion}
	}

	deadline := time.Now().Add(wait)
	for {
		page, err := s.queryAfter(ctx, cur, limit, filter)
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
				NextCursor: encodeCursorFor(cur.seq, projectionName, projectionVersion),
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
	return encodeCursorFor(seq, projection, version)
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
	if cursorStr == "" {
		head, err := s.head(ctx)
		if err != nil {
			return 0, err
		}
		return head.seq, nil
	}
	decoded, err := decodeCursorForProjection(cursorStr, projectionName, projectionVersion)
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
	if limit <= 0 || limit > maxLimit {
		limit = defaultLimit
	}
	page, err := s.queryAfter(ctx, cursor{seq: fromSeq}, limit, filter)
	if err != nil {
		return nil, err
	}
	return page.Items, nil
}

// queryAfter runs a single after-cursor SELECT against the seq column.
// seq is BIGSERIAL (strictly increasing per insert under default
// isolation), so WHERE seq > $cursor ORDER BY seq is a true monotonic
// resume primitive — no skip risk under same-microsecond contention,
// no compound-key tuple comparison, no dependency on content-addressed
// id ordering.
//
// limit+1 is fetched to compute HasMore without a separate COUNT query.
func (s *Service) queryAfter(ctx context.Context, cur cursor, limit int, filter *ProjectionFilter) (Page, error) {
	kinds := IncludedKinds
	if filter != nil {
		kinds = filter.QueryKinds()
	}
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
		scanned := 0
		for rows.Next() {
			scanned++
			item, err := scanItem(rows, true)
			if err != nil {
				rows.Close()
				return Page{}, err
			}
			scannedSeq = item.Seq
			if filter == nil || filter.Matches(item) {
				items = append(items, item)
				if len(items) > limit {
					break
				}
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return Page{}, err
		}
		rows.Close()
		if len(items) > limit || scanned < batchLimit {
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
			NextCursor: encodeCursorFor(nextSeq, cur.projection, cur.version),
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
		NextCursor: encodeCursorFor(nextSeq, cur.projection, cur.version),
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

func encodeCursorFor(seq int64, projection string, version int) string {
	if projection == "" {
		return encodeCursor(seq)
	}
	return encodeProjectionCursor(seq, projection, version)
}
