-- 0020_http_connector_stub: proof slice for approval-gated HTTP actions.

CREATE TABLE http_connector_actions (
    id              UUID        PRIMARY KEY,
    work_item_id    UUID        NOT NULL REFERENCES work_items(id) ON DELETE CASCADE,
    mode            TEXT        NOT NULL CHECK (mode IN ('read', 'write')),
    method          TEXT        NOT NULL,
    url             TEXT        NOT NULL,
    request         JSONB       NOT NULL DEFAULT '{}'::jsonb,
    status          TEXT        NOT NULL CHECK (status IN ('requested', 'awaiting_approval', 'approved', 'sent', 'failed')),
    approval_id     UUID        REFERENCES approvals(id),
    response_status INTEGER,
    response_body   TEXT        NOT NULL DEFAULT '',
    error           TEXT        NOT NULL DEFAULT '',
    requested_by    UUID        REFERENCES tokens(id),
    source          TEXT        NOT NULL CHECK (source IN ('human', 'agent', 'system')),
    created_at      TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL
);

CREATE INDEX http_connector_actions_work_item_idx ON http_connector_actions (work_item_id);
CREATE INDEX http_connector_actions_approval_idx ON http_connector_actions (approval_id);
CREATE INDEX http_connector_actions_status_idx ON http_connector_actions (status);

CREATE TABLE outbox_events (
    id          UUID        PRIMARY KEY,
    kind        TEXT        NOT NULL,
    action_id   UUID        NOT NULL REFERENCES http_connector_actions(id) ON DELETE CASCADE,
    state       TEXT        NOT NULL CHECK (state IN ('pending', 'leased', 'sent', 'failed')),
    payload     JSONB       NOT NULL DEFAULT '{}'::jsonb,
    attempts    INTEGER     NOT NULL DEFAULT 0,
    lease_until TIMESTAMPTZ,
    created_at  TIMESTAMPTZ NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL
);

CREATE INDEX outbox_events_ready_idx ON outbox_events (state, lease_until, created_at);
CREATE INDEX outbox_events_action_idx ON outbox_events (action_id);
