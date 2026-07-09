-- 0025_oauth_clients: current-state projection of dynamically-registered
-- provider OAuth clients (RFC 7591 dynamic client registration).
--
-- Truth stays in oauth_client.registered events; this table is the latest
-- registration facts per client_id for the authorization-code flow to read
-- when it validates a client + redirect_uri at /oauth/authorize.
--
-- Provider clients (vanilla Claude, ChatGPT) are public clients: they
-- authenticate with PKCE, not a client secret, so no secret material is ever
-- stored here or in the events. client_id is a non-secret random identifier.
-- redirect_uris is the exact-match allowlist; the authorize endpoint rejects
-- any redirect_uri not present. Expand-safe: new table only, no existing
-- writers touched.

CREATE TABLE oauth_clients (
    client_id                   TEXT        PRIMARY KEY,
    client_name                 TEXT        NOT NULL DEFAULT '',
    redirect_uris               JSONB       NOT NULL DEFAULT '[]'::jsonb,
    grant_types                 JSONB       NOT NULL DEFAULT '[]'::jsonb,
    response_types              JSONB       NOT NULL DEFAULT '[]'::jsonb,
    token_endpoint_auth_method  TEXT        NOT NULL,
    scope                       TEXT        NOT NULL DEFAULT '',
    created_at                  TIMESTAMPTZ NOT NULL,
    updated_at                  TIMESTAMPTZ NOT NULL
);
