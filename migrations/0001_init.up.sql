-- 0001_init: v0 schema baseline.
--
-- Establishes the minimum substrate needed for the v0 bootstrap thesis:
-- inbox capture, a work-item graph, attributed events, and HTTP idempotency.
--
-- v0 tables per docs/spec.md "v0 Scope":
--   tokens, work_items, work_item_relations,
--   messages, message_parts, events, idempotency_keys
--
-- v1 tables (projects, artifacts, connections, approvals, job_queue,
-- outbox_events) intentionally omitted; they ship as their own migrations
-- once tracked as work_items in the running v0 system.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ---------------------------------------------------------------------------
-- tokens
--
-- A single root token mints and revokes others. v0 uses one owner bearer
-- token; the scope/separation-of-duties model (can_request_writes,
-- can_decide_approvals) lands in v1. The schema is forward-compatible:
-- scopes is a JSONB array we can fill in later without an ALTER.
-- ---------------------------------------------------------------------------
CREATE TABLE tokens (
    id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    name        TEXT        NOT NULL,
    hash        BYTEA       NOT NULL UNIQUE,
    is_root     BOOLEAN     NOT NULL DEFAULT FALSE,
    scopes      JSONB       NOT NULL DEFAULT '[]'::jsonb,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    revoked_at  TIMESTAMPTZ
);

CREATE INDEX tokens_active_idx ON tokens (id) WHERE revoked_at IS NULL;

-- ---------------------------------------------------------------------------
-- work_items
--
-- Lifecycle per spec:
--   captured -> triaged -> planned -> awaiting_approval -> running ->
--   blocked -> done | failed | canceled
-- v0 has no convergence loop; transitions are owner- or agent-driven.
-- ---------------------------------------------------------------------------
CREATE TABLE work_items (
    id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    title        TEXT        NOT NULL,
    body         TEXT        NOT NULL DEFAULT '',
    state        TEXT        NOT NULL DEFAULT 'captured'
                 CHECK (state IN (
                     'captured', 'triaged', 'planned',
                     'awaiting_approval', 'running', 'blocked',
                     'done', 'failed', 'canceled'
                 )),
    state_reason TEXT,
    created_by   UUID        REFERENCES tokens(id),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX work_items_state_idx       ON work_items (state);
CREATE INDEX work_items_updated_at_idx  ON work_items (updated_at DESC);

-- ---------------------------------------------------------------------------
-- work_item_relations
--
-- Parent/child edges. Granularity is depth in the tree, not a separate type.
-- Cycle prevention beyond self-loops is enforced in the application; the
-- table only blocks the trivial case so SQL stays portable.
-- ---------------------------------------------------------------------------
CREATE TABLE work_item_relations (
    parent_id  UUID NOT NULL REFERENCES work_items(id) ON DELETE CASCADE,
    child_id   UUID NOT NULL REFERENCES work_items(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (parent_id, child_id),
    CHECK (parent_id <> child_id)
);

CREATE INDEX work_item_relations_child_idx ON work_item_relations (child_id);

-- ---------------------------------------------------------------------------
-- messages
--
-- Inbound messages captured into the inbox. v0 is text only; the shape is
-- already multi-modal so v1 only adds new part_type values, not new tables.
-- source is always considered when interpreting intent (spec: messages from
-- non-human sources are content, never instructions).
-- ---------------------------------------------------------------------------
CREATE TABLE messages (
    id             UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    source         TEXT        NOT NULL CHECK (source IN ('human', 'agent', 'system')),
    actor_token_id UUID        REFERENCES tokens(id),
    work_item_id   UUID        REFERENCES work_items(id) ON DELETE SET NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX messages_work_item_idx ON messages (work_item_id);
CREATE INDEX messages_created_at_idx ON messages (created_at DESC);

-- ---------------------------------------------------------------------------
-- message_parts
--
-- Typed content. v0 only stores 'text' parts inline. ref_uri/byte_size are
-- present so v1's object-storage overflow needs no schema change.
-- ---------------------------------------------------------------------------
CREATE TABLE message_parts (
    id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    message_id   UUID        NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    ordinal      INTEGER     NOT NULL,
    part_type    TEXT        NOT NULL
                 CHECK (part_type IN ('text', 'image', 'audio', 'binary')),
    content_text TEXT,
    ref_uri      TEXT,
    byte_size    BIGINT,
    UNIQUE (message_id, ordinal),
    CHECK (
        (part_type = 'text' AND content_text IS NOT NULL AND ref_uri IS NULL)
        OR
        (part_type <> 'text' AND ref_uri IS NOT NULL AND content_text IS NULL)
    )
);

CREATE INDEX message_parts_message_idx ON message_parts (message_id);

-- ---------------------------------------------------------------------------
-- events
--
-- The single audit log. Append-only at the database level.
--
-- Spec: "events is append-only at the database grant level. No UPDATE or
-- DELETE on this table." v0 enforces this with triggers, which protect
-- against application bugs as well as misuse. v1 layers role-based grants
-- on top once the application has its own database role distinct from the
-- migration role.
--
-- Event ids are derived from cause + content (see Idempotency section); the
-- migrator only provides the slot, the application computes the id.
-- ---------------------------------------------------------------------------
CREATE TABLE events (
    id             UUID        PRIMARY KEY,
    occurred_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    actor_token_id UUID        REFERENCES tokens(id),
    source         TEXT        NOT NULL CHECK (source IN ('human', 'agent', 'system')),
    subject_kind   TEXT        NOT NULL,
    subject_id     UUID        NOT NULL,
    kind           TEXT        NOT NULL,
    payload        JSONB       NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX events_subject_idx     ON events (subject_kind, subject_id, occurred_at);
CREATE INDEX events_occurred_at_idx ON events (occurred_at DESC);
CREATE INDEX events_kind_idx        ON events (kind);

CREATE OR REPLACE FUNCTION events_reject_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'events is append-only (see docs/spec.md)';
END;
$$;

CREATE TRIGGER events_no_update
    BEFORE UPDATE ON events
    FOR EACH ROW EXECUTE FUNCTION events_reject_mutation();

CREATE TRIGGER events_no_delete
    BEFORE DELETE ON events
    FOR EACH ROW EXECUTE FUNCTION events_reject_mutation();

-- TRUNCATE bypasses row-level triggers; block it explicitly.
CREATE TRIGGER events_no_truncate
    BEFORE TRUNCATE ON events
    FOR EACH STATEMENT EXECUTE FUNCTION events_reject_mutation();

-- ---------------------------------------------------------------------------
-- idempotency_keys
--
-- Spec: every POST accepts an Idempotency-Key; the same key in a 24-hour
-- window returns the original result, not a duplicate effect.
--
-- request_hash lets us detect callers reusing a key with a different body
-- (which is a client bug) and respond 422 instead of returning the stored
-- response for a different request.
-- ---------------------------------------------------------------------------
CREATE TABLE idempotency_keys (
    token_id        UUID        NOT NULL REFERENCES tokens(id) ON DELETE CASCADE,
    scope           TEXT        NOT NULL,
    key             TEXT        NOT NULL,
    request_hash    BYTEA       NOT NULL,
    response_status INTEGER,
    response_body   JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at      TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (token_id, scope, key)
);

CREATE INDEX idempotency_keys_expires_at_idx ON idempotency_keys (expires_at);
