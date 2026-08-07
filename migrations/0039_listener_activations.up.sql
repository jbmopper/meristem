-- 0039_listener_activations: restart-safe delivery state for listener adapters.
-- Truth remains listener.activation_* events; this table is only their
-- synchronous projection. External application contact is always preceded by
-- a dispatching event carrying a finite lease and deterministic consumer
-- generation. An expired dispatch becomes ambiguous and may only reconcile.

CREATE TABLE listener_activations (
    id                    UUID        PRIMARY KEY,
    listener_id           UUID        NOT NULL REFERENCES listener_registrations(id),
    work_item_id          UUID        NOT NULL REFERENCES work_items(id),
    assignment_event_id   UUID        NOT NULL REFERENCES events(id),
    demand_event_id       UUID        NOT NULL REFERENCES events(id),
    attempt               INTEGER     NOT NULL CHECK (attempt >= 1),
    adapter_kind          TEXT        NOT NULL CHECK (adapter_kind <> ''),
    binding_generation    TEXT        NOT NULL CHECK (binding_generation <> ''),
    state                 TEXT        NOT NULL CHECK (state IN (
        'requested', 'dispatching', 'accepted', 'completed', 'failed', 'ambiguous'
    )),
    dispatch_mode         TEXT        CHECK (dispatch_mode IS NULL OR dispatch_mode IN ('dispatch', 'reconcile')),
    consumer_generation   TEXT,
    lease_expires_at      TIMESTAMPTZ,
    dispatch_count        INTEGER     NOT NULL DEFAULT 0 CHECK (dispatch_count >= 0),
    reconcile_count       INTEGER     NOT NULL DEFAULT 0 CHECK (reconcile_count >= 0),
    next_retry_at         TIMESTAMPTZ,
    last_reason           TEXT        NOT NULL DEFAULT '',
    last_outcome_event_id UUID        NOT NULL REFERENCES events(id),
    state_event_id        UUID        NOT NULL REFERENCES events(id),
    state_event_seq       BIGINT      NOT NULL,
    created_at            TIMESTAMPTZ NOT NULL,
    updated_at            TIMESTAMPTZ NOT NULL,
    UNIQUE (assignment_event_id, binding_generation, attempt),
    CHECK (
        (state IN ('dispatching', 'accepted') AND dispatch_mode IS NOT NULL AND consumer_generation IS NOT NULL AND lease_expires_at IS NOT NULL)
        OR
        (state NOT IN ('dispatching', 'accepted') AND dispatch_mode IS NULL AND consumer_generation IS NULL AND lease_expires_at IS NULL)
    )
);

CREATE INDEX listener_activations_listener_state_idx
    ON listener_activations (listener_id, state, next_retry_at, created_at, id);

CREATE INDEX listener_activations_assignment_idx
    ON listener_activations (assignment_event_id, attempt DESC);
