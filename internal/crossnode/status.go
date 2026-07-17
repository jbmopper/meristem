package crossnode

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Status reads for the operator diagnostics surface (`meristem node status`,
// work item 0b5d514b). Everything in this file is a read of existing
// projections — command_queue, crossnode_outcome_observations,
// crossnode_outcome_cursors, spoke_state — exactly as the projectors folded
// them. No event kinds, no mutations, no network. The one truth these reads
// cannot show is sender route cooldowns: those are process-local hints inside
// a delivering sender (see CooldownWindow) and are deliberately not shared
// state.
//
// Callers pass a Querier; `node status` passes a repeatable-read read-only
// transaction so every section reports one coherent snapshot and the database
// itself enforces that the diagnostic cannot write.

// Querier is the read surface the status queries need. Both *pgxpool.Pool and
// pgx.Tx satisfy it.
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// QueueTargetStatus summarizes one target's durable command queue on this
// node: what is waiting and in which retry state, when patience runs out, and
// the most recent terminal outcome and most recent failure.
type QueueTargetStatus struct {
	TargetNodeID string `json:"target_node_id"`

	// Pending = PendingRetryable + PendingExhausted + PendingDue, evaluated
	// at the caller-supplied instant:
	//   retryable — attempts remain and the deadline has not passed; the
	//     command retries on the target's next drain tick.
	//   exhausted — all MaxCommandAttempts local attempts are spent before
	//     the deadline; the queue-host attempt gate refuses further
	//     execution and the row waits for the expiry worker.
	//   due — the 24h deadline has passed; the row is eligible for the
	//     expiry worker's next pass. Expiry is an eligibility deadline, not
	//     a transition timestamp: the terminal expired fact lands when the
	//     worker processes the row.
	Pending          int        `json:"pending"`
	PendingRetryable int        `json:"pending_retryable"`
	PendingExhausted int        `json:"pending_exhausted"`
	PendingDue       int        `json:"pending_due"`
	OldestQueuedAt   *time.Time `json:"oldest_queued_at,omitempty"`
	NextExpiresAt    *time.Time `json:"next_expires_at,omitempty"`
	// MaxAttempts is the highest attempt_count over pending rows, out of
	// MaxCommandAttempts; LastAttemptAt is the most recent local execution
	// attempt over pending rows.
	MaxAttempts   int        `json:"max_attempts"`
	LastAttemptAt *time.Time `json:"last_attempt_at,omitempty"`

	// Terminal tallies over the whole projection.
	Done    int `json:"done"`
	Refused int `json:"refused"`
	Failed  int `json:"failed"`
	Expired int `json:"expired"`

	// LastTerminal is the most recent terminal row for this target in
	// authoritative event order (the terminal event's seq); LastFailure is
	// the most recent refused/failed/expired row in the same order, retained
	// even when later commands succeeded. Nil when no such row exists.
	LastTerminal *TerminalOutcome `json:"last_terminal,omitempty"`
	LastFailure  *TerminalOutcome `json:"last_failure,omitempty"`
}

// TerminalOutcome names one terminal fact for a target: which command, how it
// ended, and when. At is the terminal event's occurred_at as folded into
// acked_at (both the ack and expiry projectors stamp it); ordering between
// outcomes uses the terminal event's seq, never wall-clock ties.
type TerminalOutcome struct {
	CommandQueueID uuid.UUID `json:"command_queue_id"`
	CommandPath    string    `json:"command_path"`
	State          string    `json:"state"`
	Reason         string    `json:"reason,omitempty"`
	StatusCode     *int      `json:"status_code,omitempty"`
	At             time.Time `json:"at"`
}

// OutcomeHostStatus is the origin-side view of one queue host: how far the
// outcome reconciler has read (cursor), and the last terminal fact observed
// in remote event order.
type OutcomeHostStatus struct {
	QueueHostNodeID string    `json:"queue_host_node_id"`
	OriginNodeID    string    `json:"origin_node_id"`
	CursorSeq       int64     `json:"cursor_seq"`
	CursorUpdatedAt time.Time `json:"cursor_updated_at"`
	Observations    int       `json:"observations"`

	LastObserved *ObservedOutcome `json:"last_observed,omitempty"`
}

// ObservedOutcome is the most recent command_outcome.observed fact from one
// queue host.
type ObservedOutcome struct {
	CommandQueueID   uuid.UUID `json:"command_queue_id"`
	TargetNodeID     string    `json:"target_node_id"`
	Outcome          string    `json:"outcome"`
	StatusCode       *int      `json:"status_code,omitempty"`
	Reason           string    `json:"reason,omitempty"`
	RemoteOccurredAt time.Time `json:"remote_occurred_at"`
}

// QueueStatus reads the per-target durable-queue summary from the
// command_queue projection, ordered by target node id. now is the instant the
// pending retryable/exhausted/due split is evaluated at.
func QueueStatus(ctx context.Context, q Querier, now time.Time) ([]QueueTargetStatus, error) {
	rows, err := q.Query(ctx, `
		SELECT target_node_id,
		       COUNT(*) FILTER (WHERE state = 'pending')::int,
		       COUNT(*) FILTER (WHERE state = 'pending' AND expires_at > $1 AND attempt_count < $2)::int,
		       COUNT(*) FILTER (WHERE state = 'pending' AND expires_at > $1 AND attempt_count >= $2)::int,
		       COUNT(*) FILTER (WHERE state = 'pending' AND expires_at <= $1)::int,
		       MIN(queued_at) FILTER (WHERE state = 'pending'),
		       MIN(expires_at) FILTER (WHERE state = 'pending'),
		       COALESCE(MAX(attempt_count) FILTER (WHERE state = 'pending'), 0)::int,
		       MAX(last_attempt_at) FILTER (WHERE state = 'pending'),
		       COUNT(*) FILTER (WHERE state = 'done')::int,
		       COUNT(*) FILTER (WHERE state = 'refused')::int,
		       COUNT(*) FILTER (WHERE state = 'failed')::int,
		       COUNT(*) FILTER (WHERE state = 'expired')::int
		FROM command_queue
		GROUP BY target_node_id
		ORDER BY target_node_id
	`, now, MaxCommandAttempts)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []QueueTargetStatus
	byTarget := make(map[string]int)
	for rows.Next() {
		var s QueueTargetStatus
		if err := rows.Scan(
			&s.TargetNodeID,
			&s.Pending, &s.PendingRetryable, &s.PendingExhausted, &s.PendingDue,
			&s.OldestQueuedAt, &s.NextExpiresAt,
			&s.MaxAttempts, &s.LastAttemptAt,
			&s.Done, &s.Refused, &s.Failed, &s.Expired,
		); err != nil {
			return nil, err
		}
		byTarget[s.TargetNodeID] = len(out)
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Most recent terminal row and most recent failure per target, both in
	// authoritative event order: the terminal event's seq in this node's log,
	// never acked_at wall-clock ties. A later done must not hide the last
	// failure, so the failure query filters before ranking.
	assign := func(sql string, set func(i int, t TerminalOutcome)) error {
		terminal, err := q.Query(ctx, sql)
		if err != nil {
			return err
		}
		defer terminal.Close()
		for terminal.Next() {
			var target string
			var t TerminalOutcome
			if err := terminal.Scan(&target, &t.CommandQueueID, &t.CommandPath, &t.State, &t.Reason, &t.StatusCode, &t.At); err != nil {
				return err
			}
			if i, ok := byTarget[target]; ok {
				set(i, t)
			}
		}
		return terminal.Err()
	}
	if err := assign(`
		SELECT DISTINCT ON (cq.target_node_id)
		       cq.target_node_id, cq.id, cq.command_path, cq.state,
		       COALESCE(cq.terminal_reason, ''), cq.outcome_status_code, cq.acked_at
		FROM command_queue cq
		JOIN events e ON e.id = cq.terminal_event_id
		WHERE cq.state <> 'pending' AND cq.acked_at IS NOT NULL
		ORDER BY cq.target_node_id, e.seq DESC
	`, func(i int, t TerminalOutcome) { last := t; out[i].LastTerminal = &last }); err != nil {
		return nil, err
	}
	if err := assign(`
		SELECT DISTINCT ON (cq.target_node_id)
		       cq.target_node_id, cq.id, cq.command_path, cq.state,
		       COALESCE(cq.terminal_reason, ''), cq.outcome_status_code, cq.acked_at
		FROM command_queue cq
		JOIN events e ON e.id = cq.terminal_event_id
		WHERE cq.state IN ('refused', 'failed', 'expired') AND cq.acked_at IS NOT NULL
		ORDER BY cq.target_node_id, e.seq DESC
	`, func(i int, t TerminalOutcome) { last := t; out[i].LastFailure = &last }); err != nil {
		return nil, err
	}
	return out, nil
}

// OutcomeStatus reads the origin-side reconciler view: one row per
// (queue host, origin) cursor plus that host's most recent observation in
// remote event order.
func OutcomeStatus(ctx context.Context, q Querier) ([]OutcomeHostStatus, error) {
	rows, err := q.Query(ctx, `
		SELECT c.queue_host_node_id, c.origin_node_id, c.remote_event_seq, c.updated_at,
		       (SELECT COUNT(*)::int
		          FROM crossnode_outcome_observations o
		         WHERE o.queue_host_node_id = c.queue_host_node_id
		           AND o.origin_node_id = c.origin_node_id)
		FROM crossnode_outcome_cursors c
		ORDER BY c.queue_host_node_id, c.origin_node_id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []OutcomeHostStatus
	byHost := make(map[string]int)
	for rows.Next() {
		var s OutcomeHostStatus
		if err := rows.Scan(&s.QueueHostNodeID, &s.OriginNodeID, &s.CursorSeq, &s.CursorUpdatedAt, &s.Observations); err != nil {
			return nil, err
		}
		byHost[s.QueueHostNodeID+"|"+s.OriginNodeID] = len(out)
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	last, err := q.Query(ctx, `
		SELECT DISTINCT ON (queue_host_node_id, origin_node_id)
		       queue_host_node_id, origin_node_id,
		       command_queue_id, target_node_id, outcome, status_code,
		       COALESCE(terminal_reason, ''), remote_occurred_at
		FROM crossnode_outcome_observations
		ORDER BY queue_host_node_id, origin_node_id, remote_event_seq DESC
	`)
	if err != nil {
		return nil, err
	}
	defer last.Close()
	for last.Next() {
		var host, origin string
		var o ObservedOutcome
		if err := last.Scan(&host, &origin, &o.CommandQueueID, &o.TargetNodeID, &o.Outcome, &o.StatusCode, &o.Reason, &o.RemoteOccurredAt); err != nil {
			return nil, err
		}
		if i, ok := byHost[host+"|"+origin]; ok {
			obs := o
			out[i].LastObserved = &obs
		}
	}
	if err := last.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// SpokeCursor is one spoke_state bookmark (the durable hub-feed cursor a
// pull-only node resumes from).
type SpokeCursor struct {
	Key       string    `json:"key"`
	Value     string    `json:"value"`
	UpdatedAt time.Time `json:"updated_at"`
}

// SpokeCursors reads the spoke_state bookmarks. An empty result on a hub is
// normal: only pull-only nodes advance spoke cursors.
func SpokeCursors(ctx context.Context, q Querier) ([]SpokeCursor, error) {
	rows, err := q.Query(ctx, `SELECT key, value, updated_at FROM spoke_state ORDER BY key`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SpokeCursor
	for rows.Next() {
		var c SpokeCursor
		if err := rows.Scan(&c.Key, &c.Value, &c.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
