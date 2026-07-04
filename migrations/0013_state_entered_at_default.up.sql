-- 0013_state_entered_at_default: expand-safety for 0012.
--
-- 0012 left state_entered_at NOT NULL with no default, which breaks
-- work_items inserts from binaries built before the projector learned the
-- column (confirmed live on 2026-07-04). New code always writes the value
-- explicitly, so this default only serves in-flight older processes during
-- upgrades; for them, "the row was created now" is exactly the right epoch.

ALTER TABLE work_items
    ALTER COLUMN state_entered_at SET DEFAULT now();
