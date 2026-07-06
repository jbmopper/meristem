-- 0019_approvals: event-sourced approval read model.

CREATE TABLE approvals (
    id              UUID        PRIMARY KEY,
    work_item_id    UUID        NOT NULL REFERENCES work_items(id) ON DELETE CASCADE,
    status          TEXT        NOT NULL CHECK (status IN ('pending', 'approved', 'denied', 'expired')),
    summary         TEXT        NOT NULL DEFAULT '',
    request         JSONB       NOT NULL DEFAULT '{}'::jsonb,
    requested_by    UUID        REFERENCES tokens(id),
    requested_source TEXT       NOT NULL CHECK (requested_source IN ('human', 'agent', 'system')),
    created_at      TIMESTAMPTZ NOT NULL,
    expires_at      TIMESTAMPTZ NOT NULL,
    decided_by      UUID        REFERENCES tokens(id),
    decision_source TEXT        CHECK (decision_source IN ('human', 'agent', 'system')),
    decided_at      TIMESTAMPTZ,
    decision        TEXT        CHECK (decision IN ('approved', 'denied', 'expired')),
    decision_reason TEXT        NOT NULL DEFAULT '',
    updated_at      TIMESTAMPTZ NOT NULL
);

CREATE INDEX approvals_work_item_idx ON approvals (work_item_id);
CREATE INDEX approvals_status_expires_idx ON approvals (status, expires_at);
