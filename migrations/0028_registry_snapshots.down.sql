DROP TABLE IF EXISTS registry_snapshot_state;

ALTER TABLE nodes DROP COLUMN IF EXISTS registry_revision;
