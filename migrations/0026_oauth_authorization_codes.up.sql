-- 0026_oauth_authorization_codes: current-state projection of issued OAuth
-- authorization codes for the provider /mcp authorization-code + PKCE flow.
--
-- Truth stays in oauth_authorization_code.issued / .redeemed events; this
-- table is the latest state per code for the token endpoint to read when it
-- exchanges a code for an access token.
--
-- The raw authorization code is a secret shown once to the client in the
-- redirect; only its SHA-256 hash is stored here (code_hash), mirroring how
-- the tokens table stores a secret hash, never the secret. code_id is a
-- deterministic non-secret id derived from the code hash so events about one
-- code share a subject. One-time use is enforced by redeemed_at: the token
-- endpoint redeems a code exactly once (redeemed_at IS NULL guard + a
-- deterministic redeemed event). expires_at bounds the short code lifetime.
--
-- code_challenge/code_challenge_method carry the PKCE binding (S256 only);
-- the token endpoint recomputes the challenge from the client's code_verifier.
-- resource is the audience the eventual access token is bound to (/mcp).
-- actor_token_id is the meristem actor the minted access token will attribute
-- to, set at consent time so provider traffic never collapses to one shared
-- actor. Expand-safe: new table only.

CREATE TABLE oauth_authorization_codes (
    code_id                 TEXT        PRIMARY KEY,
    code_hash               BYTEA       NOT NULL UNIQUE,
    client_id               TEXT        NOT NULL,
    redirect_uri            TEXT        NOT NULL,
    code_challenge          TEXT        NOT NULL,
    code_challenge_method   TEXT        NOT NULL,
    scope                   TEXT        NOT NULL DEFAULT '',
    resource                TEXT        NOT NULL,
    actor_token_id          UUID        NOT NULL,
    expires_at              TIMESTAMPTZ NOT NULL,
    redeemed_at             TIMESTAMPTZ,
    created_at              TIMESTAMPTZ NOT NULL,
    updated_at              TIMESTAMPTZ NOT NULL
);
