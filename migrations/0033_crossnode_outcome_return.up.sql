-- 0033_crossnode_outcome_return: origin-local observations of terminal
-- queue-host outcomes and their durable per-host cursor. Both tables are
-- deterministic projections of command_outcome.observed events.

CREATE TABLE crossnode_outcome_observations (
    queue_host_node_id       TEXT        NOT NULL,
    origin_node_id           TEXT        NOT NULL,
    command_queue_id         UUID        NOT NULL,
    target_node_id           TEXT        NOT NULL,
    causing_work_item_id     UUID,
    remote_event_seq         BIGINT      NOT NULL CHECK (remote_event_seq > 0),
    remote_terminal_event_id UUID        NOT NULL,
    outcome                  TEXT        NOT NULL CHECK (outcome IN ('done', 'refused', 'failed', 'expired')),
    status_code              INTEGER,
    terminal_reason          TEXT,
    cause_resolution         TEXT        NOT NULL CHECK (cause_resolution IN ('none', 'local_work_item_failed', 'local_work_item_already_terminal', 'local_work_item_missing')),
    remote_occurred_at       TIMESTAMPTZ NOT NULL,
    observed_event_id        UUID        NOT NULL,
    observed_at              TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (queue_host_node_id, command_queue_id),
    UNIQUE (queue_host_node_id, remote_terminal_event_id)
);

CREATE TABLE crossnode_outcome_cursors (
    queue_host_node_id TEXT        NOT NULL,
    origin_node_id     TEXT        NOT NULL,
    remote_event_seq   BIGINT      NOT NULL CHECK (remote_event_seq > 0),
    updated_at         TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (queue_host_node_id, origin_node_id)
);
