ALTER TABLE oauth_clients
    ADD COLUMN actor_token_id UUID REFERENCES tokens(id),
    ADD COLUMN authority_profile TEXT NOT NULL DEFAULT '',
    ADD COLUMN binding_work_item_id UUID REFERENCES work_items(id),
    ADD COLUMN revoked_at TIMESTAMPTZ;

ALTER TABLE oauth_authorization_codes
    ADD COLUMN authority_profile TEXT NOT NULL DEFAULT '';

CREATE INDEX oauth_clients_actor_idx ON oauth_clients (actor_token_id);
