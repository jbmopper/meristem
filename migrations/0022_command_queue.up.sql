-- 0022_command_queue: per-target durable queue of cross-node commands.
--
-- Truth stays in command.queued events (see docs/network-layer-spec.md §2
-- "Commands to nodes without inbound reachability" and §2b step 3 "Durable
-- queue"). This table is the current-state projection a target node reads to
-- drain its queue by outbound poll and execute each command locally under its
-- own agent token with the original idempotency key so replays collapse.
--
-- One row per command.queued event, keyed on the deterministic event id so a
-- replayed queue POST folds to the same row (ON CONFLICT DO NOTHING in the
-- projector). target_node_id is the DNS-safe home node the command is bound
-- for; command_path/command_body are the home-node REST call to replay;
-- origin_idempotency_key and origin_actor_token_id carry the originating
-- request's attribution across the boundary.
--
-- Expand-safe: new table only, no existing writers touched, and the events
-- append-only triggers are untouched. Drain/ack bookkeeping (claim, executed,
-- acknowledged) lands with the spoke-drain slice (work item bc1da2c5).

CREATE TABLE command_queue (
    id                     UUID        PRIMARY KEY,
    target_node_id         TEXT        NOT NULL,
    command_path           TEXT        NOT NULL,
    command_body           JSONB       NOT NULL,
    origin_idempotency_key TEXT        NOT NULL,
    origin_actor_token_id  UUID,
    queued_at              TIMESTAMPTZ NOT NULL
);

-- The drain path reads a single target's queue oldest-first.
CREATE INDEX command_queue_target_idx ON command_queue (target_node_id, queued_at);
