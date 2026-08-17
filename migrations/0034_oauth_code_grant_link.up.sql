-- 0034_oauth_code_grant_link: link a redeemed authorization code to the grant
-- minted from it so a later code replay can revoke that grant (RFC 6749
-- §4.1.2). Truth stays in events: oauth_authorization_code.redeemed now carries
-- the grant_id and folds it here; the token endpoint reads this column when a
-- replay is detected to revoke the compromised grant. Nullable and unconstrained
-- (no FK): the redeemed event is folded before the grant.issued event in the
-- same redemption, and codes redeemed before this column existed keep NULL.
-- Expand-safe: new nullable column only.

ALTER TABLE oauth_authorization_codes
    ADD COLUMN grant_id UUID;
