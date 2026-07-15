DROP INDEX IF EXISTS oauth_grants_code_id_key;
ALTER TABLE oauth_grants DROP COLUMN IF EXISTS code_id;
