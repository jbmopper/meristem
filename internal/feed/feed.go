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
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/wayline/internal/domain"
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

// List returns the most recent feed-visible events, newest first. The kind
// filter is parameterized off IncludedKinds rather than inlined so the
// policy lives in one place (and a parity test can assert it stays in
// sync with the domain's known event kinds).
func (s *Service) List(ctx context.Context, limit int) ([]Item, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
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
