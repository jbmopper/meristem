DROP INDEX IF EXISTS oauth_clients_actor_idx;
ALTER TABLE oauth_authorization_codes DROP COLUMN IF EXISTS authority_profile;
ALTER TABLE oauth_clients DROP COLUMN IF EXISTS revoked_at, DROP COLUMN IF EXISTS binding_work_item_id, DROP COLUMN IF EXISTS authority_profile, DROP COLUMN IF EXISTS actor_token_id;
