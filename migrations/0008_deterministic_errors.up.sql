-- 0008_deterministic_errors: maskable error reports for the deterministic layer.
--
-- This table is a projection of deterministic_error.* events. Masking is
-- display policy for active error views; the audit remains in events.

CREATE TABLE deterministic_errors (
    id          UUID        PRIMARY KEY,
    component   TEXT        NOT NULL,
    code        TEXT        NOT NULL,
    message     TEXT        NOT NULL,
    severity    TEXT        NOT NULL
                CHECK (severity IN ('info', 'warning', 'error', 'critical')),
    details     JSONB       NOT NULL DEFAULT '{}'::jsonb,
    reported_by UUID        REFERENCES tokens(id),
    reported_at TIMESTAMPTZ NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL,
    masked      BOOLEAN     NOT NULL DEFAULT FALSE,
    mask_reason TEXT,
    masked_by   UUID        REFERENCES tokens(id),
    masked_at   TIMESTAMPTZ
);

CREATE INDEX deterministic_errors_active_idx
    ON deterministic_errors (updated_at DESC)
    WHERE masked = FALSE;

CREATE INDEX deterministic_errors_masked_idx
    ON deterministic_errors (masked, updated_at DESC);

CREATE INDEX deterministic_errors_component_code_idx
    ON deterministic_errors (component, code);
