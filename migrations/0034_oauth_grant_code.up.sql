-- 0034_oauth_grant_code: link each OAuth grant back to the authorization code
-- it was minted from, so an authorization-code replay can revoke the grant that
-- the code's first (legitimate) redemption issued (RFC 6749 §4.1.2 / RFC 9700),
-- mirroring how refresh-token reuse revokes the whole grant.
--
-- Truth stays in the oauth_grant.issued event, which now carries code_id; this
-- column is the current-state projection the token endpoint reads when it
-- detects a redeemed code and needs the grant to revoke. The partial unique
-- index enforces the one-code-one-grant invariant (refresh events never mint a
-- new grant, so at most one grant carries a given code_id). Expand-safe:
-- additive column with a default for any pre-existing rows.

ALTER TABLE oauth_grants ADD COLUMN code_id TEXT NOT NULL DEFAULT '';
CREATE UNIQUE INDEX oauth_grants_code_id_key ON oauth_grants (code_id) WHERE code_id <> '';
