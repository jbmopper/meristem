-- 0017_projection_definitions: R6 named feed projections.

CREATE TABLE projections (
    name            TEXT        PRIMARY KEY,
    version         INTEGER     NOT NULL CHECK (version >= 1),
    projection_type TEXT        NOT NULL CHECK (projection_type IN ('feed')),
    rootstock       BOOLEAN     NOT NULL DEFAULT FALSE,
    filter          JSONB       NOT NULL,
    description     TEXT        NOT NULL DEFAULT '',
    event_id        UUID        NOT NULL REFERENCES events(id),
    defined_at      TIMESTAMPTZ NOT NULL,
    defined_by      UUID        REFERENCES tokens(id),
    source          TEXT        NOT NULL CHECK (source IN ('human', 'agent', 'system'))
);

CREATE INDEX projections_type_idx ON projections (projection_type);
