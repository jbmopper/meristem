-- 0012_work_item_state_entered_at: separate lifecycle dwell from activity.
--
-- updated_at remains the activity timestamp. state_entered_at is the state
-- epoch anchor used by the patience metronome; progress chatter must not reset
-- it.

ALTER TABLE work_items
    ADD COLUMN state_entered_at TIMESTAMPTZ;

WITH latest_state_epoch AS (
    SELECT DISTINCT ON (subject_id)
        subject_id AS work_item_id,
        occurred_at AS state_entered_at
    FROM events
    WHERE subject_kind = 'work_item'
      AND (
          kind = 'work_item.created'
          OR (
              kind = 'work_item.transitioned'
              AND COALESCE(payload->>'from', '') <> COALESCE(payload->>'to', '')
          )
      )
    ORDER BY subject_id, occurred_at DESC, id DESC
)
UPDATE work_items wi
SET state_entered_at = latest_state_epoch.state_entered_at
FROM latest_state_epoch
WHERE wi.id = latest_state_epoch.work_item_id
  AND wi.state_entered_at IS NULL;

UPDATE work_items
SET state_entered_at = updated_at
WHERE state_entered_at IS NULL;

ALTER TABLE work_items
    ALTER COLUMN state_entered_at SET NOT NULL;

CREATE INDEX work_items_state_entered_at_idx
    ON work_items (state, state_entered_at ASC);
