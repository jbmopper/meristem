-- 0016_registry: current-state projections for R2 tropisms and cultivars.
--
-- Truth stays in tropism.defined and cultivar.defined events. These tables
-- hold the latest version per name for reads, validation, and worker launch
-- lookup.

CREATE TABLE tropisms (
    name              TEXT        PRIMARY KEY,
    version           INTEGER     NOT NULL CHECK (version > 0),
    reducer_identity  TEXT        NOT NULL,
    reducer_version   INTEGER     NOT NULL CHECK (reducer_version > 0),
    params            JSONB       NOT NULL DEFAULT '{}'::jsonb,
    description       TEXT        NOT NULL DEFAULT '',
    event_id          UUID        NOT NULL REFERENCES events(id),
    defined_at        TIMESTAMPTZ NOT NULL,
    defined_by        UUID        REFERENCES tokens(id),
    source            TEXT        NOT NULL CHECK (source IN ('human', 'agent', 'system'))
);

CREATE INDEX tropisms_reducer_idx
    ON tropisms (reducer_identity, reducer_version);

CREATE TABLE cultivars (
    name             TEXT        PRIMARY KEY,
    version          INTEGER     NOT NULL CHECK (version > 0),
    rootstock        BOOLEAN     NOT NULL DEFAULT FALSE,
    tropism_name     TEXT        NOT NULL,
    tropism_version  INTEGER     NOT NULL CHECK (tropism_version > 0),
    profile          JSONB       NOT NULL DEFAULT '{}'::jsonb,
    xylem            JSONB       NOT NULL DEFAULT '{}'::jsonb,
    phloem           TEXT        NOT NULL,
    description      TEXT        NOT NULL DEFAULT '',
    event_id         UUID        NOT NULL REFERENCES events(id),
    defined_at       TIMESTAMPTZ NOT NULL,
    defined_by       UUID        REFERENCES tokens(id),
    source           TEXT        NOT NULL CHECK (source IN ('human', 'agent', 'system'))
);

CREATE INDEX cultivars_tropism_idx
    ON cultivars (tropism_name, tropism_version);

CREATE INDEX cultivars_rootstock_idx
    ON cultivars (rootstock);
