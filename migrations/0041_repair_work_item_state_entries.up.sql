-- 0041_repair_work_item_state_entries: align the persisted lifecycle epoch
-- projection with the authoritative state-entry fold.
--
-- Older transitionedProjector versions advanced state_entered_at whenever a
-- legacy work_item.transitioned payload omitted `from`, even when its `to`
-- state equaled the preceding lifecycle result. The canonical reducer treats
-- that fact as a same-state no-op. Repair every node deterministically from
-- its immutable home-node event log before the new projector begins serving.

LOCK TABLE events IN SHARE MODE;

CREATE TEMP TABLE meristem_0041_latest_state_entry
ON COMMIT DROP
AS
WITH lifecycle AS (
    SELECT
        fact.*,
        lag(fact.state) OVER (PARTITION BY fact.subject_id ORDER BY fact.seq) AS prior_state
    FROM (
        SELECT
            e.subject_id,
            e.seq,
            e.kind,
            e.occurred_at,
            CASE
                WHEN e.kind = 'work_item.created'
                THEN COALESCE(NULLIF(e.payload->>'state', ''), 'captured')
                ELSE NULLIF(e.payload->>'to', '')
            END AS state
        FROM events e
        WHERE e.subject_kind = 'work_item'
          AND e.kind IN ('work_item.created', 'work_item.transitioned')
          AND jsonb_typeof(e.payload) = 'object'
    ) fact
),
state_entries AS (
    SELECT subject_id, seq, state, occurred_at
    FROM lifecycle
    WHERE state IS NOT NULL
      AND (kind = 'work_item.created' OR state IS DISTINCT FROM prior_state)
)
SELECT DISTINCT ON (subject_id)
    subject_id AS work_item_id,
    state,
    occurred_at AS state_entered_at
FROM state_entries
ORDER BY subject_id, seq DESC;

-- A lifecycle-state mismatch is outside this timestamp-only repair's safe
-- scope: changing state without replaying all transition side effects could
-- fabricate projection truth. Fail the migration for operator review instead.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM work_items wi
        LEFT JOIN meristem_0041_latest_state_entry latest
          ON latest.work_item_id = wi.id
        WHERE latest.work_item_id IS NULL
           OR latest.state IS DISTINCT FROM wi.state
    ) THEN
        RAISE EXCEPTION '0041: work_items lifecycle state disagrees with authoritative event fold';
    END IF;
END
$$;

UPDATE work_items wi
SET state_entered_at = latest.state_entered_at
FROM meristem_0041_latest_state_entry latest
WHERE wi.id = latest.work_item_id
  AND wi.state_entered_at IS DISTINCT FROM latest.state_entered_at;
