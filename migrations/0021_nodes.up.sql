-- 0021_nodes: current-state projection of the fleet node registry.
--
-- Truth stays in node.registered and node.route_updated events (see
-- docs/network-layer-spec.md §2 "Naming" and §6 stage 0). This table holds
-- the latest reachability state per node for reads and route selection.
--
-- node_id is the stable, DNS-safe fleet identifier (e.g. `m4`, `den`); it is
-- the qualified-ref prefix in `<node_id>:<uuid>` cross-node references.
-- base_url is the registered ingress URL; direct_url is a direct peer route
-- when one exists; relay_via lists node ids to relay through when no direct
-- route is reachable. Expand-safe: new table only, no existing writers
-- touched, and the events append-only triggers are untouched.

CREATE TABLE nodes (
    node_id     TEXT        PRIMARY KEY,
    base_url    TEXT,
    direct_url  TEXT,
    relay_via   JSONB       NOT NULL DEFAULT '[]'::jsonb,
    status      TEXT        NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL
);
