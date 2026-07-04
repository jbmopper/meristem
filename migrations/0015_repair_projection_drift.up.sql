-- 0015_repair_projection_drift: repair live projection rows from events.
--
-- This is a deterministic repair for rows written while older binaries were
-- serving after newer projection schema/events had landed:
--   * convergence.verdict_recorded events existed, but convergence_verdicts
--     had not been populated on the live database.
--   * work_items.state_entered_at lagged state-changing transition events
--     for rows mutated by a stale API binary.
--
-- The event log is still truth. These statements fold existing events into
-- the projections using the same rules as the current projectors.

LOCK TABLE events IN SHARE MODE;

INSERT INTO convergence_verdicts (
    event_id,
    work_item_id,
    reducer_identity,
    reducer_version,
    attempt,
    inputs_digest,
    disposition,
    reason,
    signals,
    actor_token_id,
    source,
    occurred_at,
    reducer_config
)
SELECT
    e.id,
    e.subject_id,
    e.payload->>'reducer_identity',
    (e.payload->>'reducer_version')::integer,
    (e.payload->>'attempt')::integer,
    e.payload->>'inputs_digest',
    e.payload->'verdict'->>'disposition',
    e.payload->'verdict'->>'reason',
    e.payload->'signals',
    e.actor_token_id,
    e.source,
    e.occurred_at,
    COALESCE(e.payload->'reducer_config', '{}'::jsonb)
FROM events e
WHERE e.subject_kind = 'convergence'
  AND e.kind = 'convergence.verdict_recorded'
ON CONFLICT (event_id) DO UPDATE
SET work_item_id = EXCLUDED.work_item_id,
    reducer_identity = EXCLUDED.reducer_identity,
    reducer_version = EXCLUDED.reducer_version,
    attempt = EXCLUDED.attempt,
    inputs_digest = EXCLUDED.inputs_digest,
    disposition = EXCLUDED.disposition,
    reason = EXCLUDED.reason,
    signals = EXCLUDED.signals,
    actor_token_id = EXCLUDED.actor_token_id,
    source = EXCLUDED.source,
    occurred_at = EXCLUDED.occurred_at,
    reducer_config = EXCLUDED.reducer_config;

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
  AND wi.state_entered_at IS DISTINCT FROM latest_state_epoch.state_entered_at;
