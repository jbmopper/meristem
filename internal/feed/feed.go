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
	domain.EventSignalReceived,
	domain.EventPatienceBreached,
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
	Cursor string
	Wait   time.Duration
	Limit  int
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
}

// List returns the most recent feed-visible events, newest first. The kind
// filter is parameterized off IncludedKinds rather than inlined so the
// policy lives in one place (and a parity test can assert it stays in
// sync with the domain's known event kinds).
func (s *Service) List(ctx context.Context, limit int) ([]Item, error) {
	if limit <= 0 || limit > maxLimit {
		limit = defaultLimit
	}
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
// When opts.Wait > 0 and the initial after-cursor query is empty, the
// call enters bounded poll mode: a 250ms tick that re-runs the query
// until events arrive, the wait cap fires, or the request context is
// cancelled. Pool conns are released between polls (no LISTEN/NOTIFY,
// no held connection) so 1k concurrent watchers don't starve the pool.
//
// Cursor opacity is contractual; consumers that parse the encoded blob
// will break at the first encoding change. See cursor.go for the v0
// shape, which is documented as server-internal.
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

	var cur cursor
	var err error
	if opts.Cursor != "" {
		cur, err = decodeCursor(opts.Cursor)
		if err != nil {
			return Page{}, err
		}
	} else {
		cur, err = s.head(ctx)
		if err != nil {
			return Page{}, err
		}
	}

	deadline := time.Now().Add(wait)
	for {
		page, err := s.queryAfter(ctx, cur, limit)
		if err != nil {
			return Page{}, err
		}
		if len(page.Items) > 0 {
			return page, nil
		}
		if wait == 0 || !time.Now().Before(deadline) {
			return Page{
				Items:      []Item{},
				NextCursor: encodeCursor(cur.occurredAt, cur.eventID),
				HasMore:    false,
			}, nil
		}
		select {
		case <-ctx.Done():
			return Page{}, ctx.Err()
		case <-time.After(pollTick):
		}
	}
}

// head returns a cursor pointing at the most recent feed-visible event,
// or a zero-time cursor when the log is empty. Used to bootstrap
// "from now" watcher mode when no caller cursor is supplied.
func (s *Service) head(ctx context.Context) (cursor, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT occurred_at, id FROM events
		WHERE kind = ANY($1::text[])
		ORDER BY occurred_at DESC, id DESC
		LIMIT 1
	`, IncludedKinds)
	var c cursor
	if err := row.Scan(&c.occurredAt, &c.eventID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return cursor{occurredAt: time.Unix(0, 0).UTC(), eventID: uuid.Nil}, nil
		}
		return cursor{}, err
	}
	return c, nil
}

// queryAfter runs a single after-cursor SELECT. The (occurred_at, id)
// tuple comparison is the standard SQL idiom for resumable pagination
// over a compound key; it returns rows where occurred_at > cur.occurredAt,
// OR occurred_at = cur.occurredAt AND id > cur.eventID — exactly the
// "events strictly after this cursor" semantics the consumer expects.
//
// limit+1 is fetched to compute HasMore without a separate COUNT query.
func (s *Service) queryAfter(ctx context.Context, cur cursor, limit int) (Page, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, occurred_at, actor_token_id, source, subject_kind, subject_id, kind, payload
		FROM events
		WHERE kind = ANY($1::text[])
		AND (occurred_at, id) > ($2::timestamptz, $3::uuid)
		ORDER BY occurred_at ASC, id ASC
		LIMIT $4
	`, IncludedKinds, cur.occurredAt, cur.eventID, limit+1)
	if err != nil {
		return Page{}, err
	}
	defer rows.Close()
	items := make([]Item, 0, limit)
	for rows.Next() {
		var item Item
		var source string
		if err := rows.Scan(&item.EventID, &item.OccurredAt, &item.ActorTokenID, &source, &item.SubjectKind, &item.SubjectID, &item.Kind, &item.Payload); err != nil {
			return Page{}, err
		}
		item.Source = domain.Source(source)
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return Page{}, err
	}

	hasMore := len(items) > limit
	if hasMore {
		items = items[:limit]
	}
	if len(items) == 0 {
		return Page{
			Items:      []Item{},
			NextCursor: encodeCursor(cur.occurredAt, cur.eventID),
			HasMore:    false,
		}, nil
	}
	last := items[len(items)-1]
	return Page{
		Items:      items,
		NextCursor: encodeCursor(last.OccurredAt, last.EventID),
		HasMore:    hasMore,
	}, nil
}
