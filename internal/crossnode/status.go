package crossnode

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Status reads for the operator diagnostics surface (`meristem node status`,
// work item 0b5d514b). Everything in this file is a read of existing
// projections — command_queue, crossnode_outcome_observations,
// crossnode_outcome_cursors — exactly as the projectors folded them. No event
// kinds, no mutations, no network. The one truth these reads cannot show is
// sender route cooldowns: those are process-local hints inside a delivering
// sender (see CooldownWindow) and are deliberately not shared state.

// QueueTargetStatus summarizes one target's durable command queue on this
// node: what is waiting, how hard it has been tried, when patience runs out,
// and what the most recent terminal outcome was.
type QueueTargetStatus struct {
	TargetNodeID string `json:"target_node_id"`

	// Pending state: what is still waiting to drain.
	Pending        int        `json:"pending"`
	OldestQueuedAt *time.Time `json:"oldest_queued_at,omitempty"`
	// NextExpiresAt is the earliest pending expiry — the moment the queue
	// worker may turn a pending row terminal if it has not drained.
	NextExpiresAt *time.Time `json:"next_expires_at,omitempty"`
	// MaxAttempts is the highest attempt_count over pending rows, out of
	// MaxCommandAttempts. LastAttemptAt is the most recent local execution
	// attempt over pending rows; a pending row retries on the target's next
	// drain tick, so "next retry" is that tick or NextExpiresAt, whichever
	// comes first.
	MaxAttempts   int        `json:"max_attempts"`
	LastAttemptAt *time.Time `json:"last_attempt_at,omitempty"`

	// Terminal tallies over the whole projection.
	Done    int `json:"done"`
	Refused int `json:"refused"`
	Failed  int `json:"failed"`
	Expired int `json:"expired"`

	// LastTerminal is the most recent terminal row for this target, nil if
	// none has terminated yet.
	LastTerminal *TerminalOutcome `json:"last_terminal,omitempty"`
}

// TerminalOutcome names the most recent terminal fact for a target: which
// command, how it ended, and when. At is the terminal event's occurred_at as
// folded into acked_at (both the ack and expiry projectors stamp it).
type TerminalOutcome struct {
	CommandQueueID uuid.UUID `json:"command_queue_id"`
	CommandPath    string    `json:"command_path"`
	State          string    `json:"state"`
	Reason         string    `json:"reason,omitempty"`
	StatusCode     *int      `json:"status_code,omitempty"`
	At             time.Time `json:"at"`
}

// OutcomeHostStatus is the origin-side view of one queue host: how far the
// outcome reconciler has read (cursor), and the last terminal fact observed.
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
// command_queue projection, ordered by target node id.
func QueueStatus(ctx context.Context, pool *pgxpool.Pool) ([]QueueTargetStatus, error) {
	rows, err := pool.Query(ctx, `
		SELECT target_node_id,
		       COUNT(*) FILTER (WHERE state = 'pending')::int,
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
	`)
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
			&s.Pending, &s.OldestQueuedAt, &s.NextExpiresAt,
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

	terminal, err := pool.Query(ctx, `
		SELECT DISTINCT ON (target_node_id)
		       target_node_id, id, command_path, state,
		       COALESCE(terminal_reason, ''), outcome_status_code, acked_at
		FROM command_queue
		WHERE state <> 'pending' AND acked_at IS NOT NULL
		ORDER BY target_node_id, acked_at DESC, id DESC
	`)
	if err != nil {
		return nil, err
	}
	defer terminal.Close()
	for terminal.Next() {
		var target string
		var t TerminalOutcome
		if err := terminal.Scan(&target, &t.CommandQueueID, &t.CommandPath, &t.State, &t.Reason, &t.StatusCode, &t.At); err != nil {
			return nil, err
		}
		if i, ok := byTarget[target]; ok {
			last := t
			out[i].LastTerminal = &last
		}
	}
	if err := terminal.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// OutcomeStatus reads the origin-side reconciler view: one row per
// (queue host, origin) cursor plus that host's most recent observation.
func OutcomeStatus(ctx context.Context, pool *pgxpool.Pool) ([]OutcomeHostStatus, error) {
	rows, err := pool.Query(ctx, `
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

	last, err := pool.Query(ctx, `
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
func SpokeCursors(ctx context.Context, pool *pgxpool.Pool) ([]SpokeCursor, error) {
	rows, err := pool.Query(ctx, `SELECT key, value, updated_at FROM spoke_state ORDER BY key`)
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
