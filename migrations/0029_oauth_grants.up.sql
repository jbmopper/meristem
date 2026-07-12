CREATE TABLE oauth_grants (
    id UUID PRIMARY KEY,
    client_id TEXT NOT NULL,
    actor_token_id UUID NOT NULL REFERENCES tokens(id),
    authority_profile TEXT NOT NULL,
    scope TEXT NOT NULL,
    resource TEXT NOT NULL,
    refresh_expires_at TIMESTAMPTZ NOT NULL,
    revoked_at TIMESTAMPTZ,
    compromise_reason TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE oauth_access_tokens (
    token_id TEXT PRIMARY KEY,
    token_hash BYTEA NOT NULL UNIQUE,
    grant_id UUID NOT NULL REFERENCES oauth_grants(id),
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX oauth_access_tokens_expiry_idx ON oauth_access_tokens (expires_at);
CREATE TABLE oauth_refresh_tokens (
    token_id TEXT PRIMARY KEY,
    token_hash BYTEA NOT NULL UNIQUE,
    grant_id UUID NOT NULL REFERENCES oauth_grants(id),
    generation INTEGER NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    used_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL,
    UNIQUE (grant_id, generation)
);
CREATE INDEX oauth_refresh_tokens_expiry_idx ON oauth_refresh_tokens (expires_at);
