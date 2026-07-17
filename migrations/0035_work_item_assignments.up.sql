-- 0035_work_item_assignments: assignment-state projection for bounded
-- work-item holders. Truth remains work_item.assigned /
-- work_item.assignment_released in events. Every work item has one placeholder
-- row even when unassigned so competing claimers can serialize on a stable
-- SELECT ... FOR UPDATE target (an absent active assignment cannot be locked).

CREATE TABLE work_item_assignment_state (
    work_item_id        UUID        PRIMARY KEY REFERENCES work_items(id) ON DELETE CASCADE,
    holder_token_id     UUID        REFERENCES tokens(id),
    mode                TEXT        CHECK (mode IS NULL OR mode IN ('claim', 'spawn', 'handoff')),
    assignment_event_id UUID        UNIQUE REFERENCES events(id),
    claimed_at          TIMESTAMPTZ,
    expires_at          TIMESTAMPTZ,
    last_release_reason TEXT        CHECK (last_release_reason IS NULL OR last_release_reason IN ('done', 'yield', 'expired')),
    terminal_state      TEXT        CHECK (terminal_state IS NULL OR terminal_state IN ('done', 'failed', 'canceled')),
    state_event_id      UUID        NOT NULL REFERENCES events(id),
    state_event_seq     BIGINT      NOT NULL,
    updated_at          TIMESTAMPTZ NOT NULL,
    CHECK (
        (
            holder_token_id IS NULL AND mode IS NULL AND
            assignment_event_id IS NULL AND claimed_at IS NULL AND expires_at IS NULL
        ) OR (
            holder_token_id IS NOT NULL AND mode IS NOT NULL AND
            assignment_event_id IS NOT NULL AND claimed_at IS NOT NULL AND
            expires_at IS NOT NULL AND expires_at > claimed_at
        )
    ),
    CHECK (
        (last_release_reason IS NULL AND terminal_state IS NULL)
        OR (last_release_reason = 'done' AND terminal_state IS NOT NULL)
        OR (last_release_reason IN ('yield', 'expired') AND terminal_state IS NULL)
    )
);

CREATE INDEX work_item_assignment_state_expiry_idx
    ON work_item_assignment_state (expires_at, work_item_id)
    WHERE holder_token_id IS NOT NULL;

-- Guarded-cutover backfill from the latest authoritative lifecycle event.
-- Quiesce every old API/worker writer before applying this migration, then
-- start only a binary whose work_item.created projector writes the permanent
-- assignment placeholder. Current terminal rows receive a terminal sentinel;
-- non-terminal rows begin unassigned. The work_items join is a projection
-- consistency check and supplies the already-folded current lifecycle state.
INSERT INTO work_item_assignment_state (
    work_item_id, last_release_reason, terminal_state,
    state_event_id, state_event_seq, updated_at
)
SELECT wi.id,
       CASE WHEN wi.state IN ('done', 'failed', 'canceled') THEN 'done' END,
       CASE WHEN wi.state IN ('done', 'failed', 'canceled') THEN wi.state END,
       lifecycle.id, lifecycle.seq, lifecycle.occurred_at
FROM work_items wi
JOIN LATERAL (
    SELECT e.id, e.seq, e.occurred_at
    FROM events e
    WHERE e.subject_kind = 'work_item'
      AND e.subject_id = wi.id
      AND (
          (
              wi.state IN ('done', 'failed', 'canceled')
              AND e.kind IN ('work_item.created', 'work_item.transitioned')
          ) OR (
              wi.state NOT IN ('done', 'failed', 'canceled')
              AND e.kind = 'work_item.created'
          )
      )
    ORDER BY e.seq DESC
    LIMIT 1
) lifecycle ON TRUE;
