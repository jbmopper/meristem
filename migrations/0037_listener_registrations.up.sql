-- 0037_listener_registrations: projection for durable listener registrations
-- (docs/listener-control-plane.md, slice 2). Truth remains the listener.*
-- events; this table is the synchronous projection routing reads against.
-- A registration is a stable routing address bound to a rotating principal
-- credential — retiring it tombstones the row rather than deleting it, so
-- historical attribution keeps resolving.

CREATE TABLE listener_registrations (
    id                         UUID        PRIMARY KEY,
    name                       TEXT        NOT NULL UNIQUE CHECK (name <> ''),
    principal_token_id         UUID        NOT NULL REFERENCES tokens(id),
    provider                   TEXT        NOT NULL DEFAULT '',
    capabilities               JSONB       NOT NULL DEFAULT '[]'::jsonb,
    max_concurrent_assignments INTEGER     NOT NULL DEFAULT 1 CHECK (max_concurrent_assignments >= 1),
    policy                     JSONB,
    policy_fingerprint         TEXT,
    policy_event_id            UUID        REFERENCES events(id),
    retired_at                 TIMESTAMPTZ,
    state_event_id             UUID        NOT NULL REFERENCES events(id),
    state_event_seq            BIGINT      NOT NULL,
    created_at                 TIMESTAMPTZ NOT NULL,
    updated_at                 TIMESTAMPTZ NOT NULL,
    -- A policy is stored complete or not at all: fingerprint and source event
    -- travel with the payload they identify.
    CHECK (
        (policy IS NULL AND policy_fingerprint IS NULL AND policy_event_id IS NULL)
        OR (policy IS NOT NULL AND policy_fingerprint IS NOT NULL AND policy_event_id IS NOT NULL)
    )
);

CREATE INDEX listener_registrations_principal_idx
    ON listener_registrations (principal_token_id)
    WHERE retired_at IS NULL;
