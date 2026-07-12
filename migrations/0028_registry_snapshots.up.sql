-- 0028_registry_snapshots: replayable distribution of the authoritative
-- fleet node registry. The accepted source revision is a cursor in the
-- registry home's event log; registry_revision is the source revision that
-- last produced each complete snapshot entry.

ALTER TABLE nodes
    ADD COLUMN registry_revision BIGINT NOT NULL DEFAULT 0
    CHECK (registry_revision >= 0);

-- Existing node rows already have their cause in events. Backfill the new
-- projection field from that log so an upgrade and a clean rebuild agree.
UPDATE nodes AS n
SET registry_revision = COALESCE((
    SELECT MAX(e.seq)
    FROM events AS e
    WHERE e.subject_kind = 'node'
      AND e.kind IN ('node.registered', 'node.route_updated')
      AND e.payload->>'node_id' = n.node_id
), 0);

CREATE TABLE registry_snapshot_state (
    singleton       BOOLEAN     PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    source_node_id  TEXT        NOT NULL,
    source_revision BIGINT      NOT NULL CHECK (source_revision > 0),
    snapshot_digest BYTEA       NOT NULL,
    observed_at     TIMESTAMPTZ NOT NULL
);
