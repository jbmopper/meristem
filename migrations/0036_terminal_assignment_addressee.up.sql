-- 0036_terminal_assignment_addressee: preserve the exact former assignment
-- holder as a terminal-transition address. The address is a deterministic
-- projection of the assignment-control history immediately preceding the
-- terminal lifecycle event; it is bound to state_event_id so feed reads can
-- match only that exact transition rather than widening every event on a
-- terminal work item.
--
-- Guarded cutover: quiesce every 0035-era API/worker writer before applying
-- this migration, then start only the reviewed binary. The validation below
-- deliberately aborts instead of guessing if legacy lifecycle history cannot
-- identify exactly one terminal-entry event.

ALTER TABLE work_item_assignment_state
    ADD COLUMN terminal_addressee_token_id UUID REFERENCES tokens(id),
    ADD CONSTRAINT work_item_assignment_terminal_addressee_check CHECK (
        terminal_addressee_token_id IS NULL OR terminal_state IS NOT NULL
    );

DO $$
DECLARE
    invalid_work_item_id UUID;
    invalid_work_item_state TEXT;
    invalid_assignment_state TEXT;
    invalid_lifecycle_event_id UUID;
    invalid_lifecycle_state TEXT;
BEGIN
    SELECT work_item.id,
           work_item.state
    INTO invalid_work_item_id,
         invalid_work_item_state
    FROM work_items AS work_item
    LEFT JOIN work_item_assignment_state AS assignment_state
      ON assignment_state.work_item_id = work_item.id
    WHERE assignment_state.work_item_id IS NULL
    ORDER BY work_item.id
    LIMIT 1;

    IF invalid_work_item_id IS NOT NULL THEN
        RAISE EXCEPTION
            '0036 assignment placeholder missing for work_item % (state=%)',
            invalid_work_item_id,
            invalid_work_item_state;
    END IF;

    SELECT work_item.id,
           work_item.state,
           latest_lifecycle.event_id,
           latest_lifecycle.result_state
    INTO invalid_work_item_id,
         invalid_work_item_state,
         invalid_lifecycle_event_id,
         invalid_lifecycle_state
    FROM work_items AS work_item
    LEFT JOIN LATERAL (
        SELECT lifecycle.id AS event_id,
               CASE
                   WHEN lifecycle.kind = 'work_item.created'
                   THEN COALESCE(NULLIF(lifecycle.payload->>'state', ''), 'captured')
                   ELSE lifecycle.payload->>'to'
               END AS result_state
        FROM events AS lifecycle
        WHERE lifecycle.subject_kind = 'work_item'
          AND lifecycle.subject_id = work_item.id
          AND lifecycle.kind IN ('work_item.created', 'work_item.transitioned')
        ORDER BY lifecycle.seq DESC
        LIMIT 1
    ) AS latest_lifecycle ON TRUE
    WHERE latest_lifecycle.event_id IS NULL
       OR latest_lifecycle.result_state IS NULL
       OR latest_lifecycle.result_state NOT IN (
           'captured', 'triaged', 'planned', 'awaiting_approval',
           'running', 'blocked', 'done', 'failed', 'canceled'
       )
       OR work_item.state IS DISTINCT FROM latest_lifecycle.result_state
    ORDER BY work_item.id
    LIMIT 1;

    IF invalid_work_item_id IS NOT NULL THEN
        RAISE EXCEPTION
            '0036 lifecycle projection mismatch for work_item %: work_item_state=%, latest_event=%, latest_result_state=%',
            invalid_work_item_id,
            invalid_work_item_state,
            invalid_lifecycle_event_id,
            invalid_lifecycle_state;
    END IF;

    SELECT work_item.id,
           work_item.state,
           assignment_state.terminal_state
    INTO invalid_work_item_id,
         invalid_work_item_state,
         invalid_assignment_state
    FROM work_items AS work_item
    JOIN work_item_assignment_state AS assignment_state
      ON assignment_state.work_item_id = work_item.id
    WHERE (
        work_item.state IN ('done', 'failed', 'canceled')
        AND assignment_state.terminal_state IS DISTINCT FROM work_item.state
    ) OR (
        work_item.state NOT IN ('done', 'failed', 'canceled')
        AND assignment_state.terminal_state IS NOT NULL
    ) OR (
        assignment_state.terminal_state IS NOT NULL
        AND (
            assignment_state.holder_token_id IS NOT NULL
            OR assignment_state.mode IS NOT NULL
            OR assignment_state.assignment_event_id IS NOT NULL
            OR assignment_state.claimed_at IS NOT NULL
            OR assignment_state.expires_at IS NOT NULL
        )
    )
    ORDER BY work_item.id
    LIMIT 1;

    IF invalid_work_item_id IS NOT NULL THEN
        RAISE EXCEPTION
            '0036 lifecycle/assignment projection mismatch for work_item %: work_item_state=%, assignment_terminal_state=%',
            invalid_work_item_id,
            invalid_work_item_state,
            invalid_assignment_state;
    END IF;
END
$$;

-- Recover terminals written by a 0035-aware binary before this migration.
-- 0035 advanced its assignment pointer on legal terminal same-state no-ops;
-- the corrected fold keeps the pointer on the event that actually entered
-- the terminal state. Entry is derived from ordered lifecycle results, not
-- payload.from: legacy producers omitted from, including on terminal no-ops.
-- First recover that entry (including terminal-at-create), then find the
-- assignment-control event immediately preceding it. Assigned opens an epoch;
-- yield/expiry closes it. Ordinary lifecycle/progress events do not affect
-- assignment truth and are intentionally absent from the control lookup.
DO $$
DECLARE
    invalid_work_item_id UUID;
    invalid_event_id UUID;
    invalid_payload_from TEXT;
    invalid_prior_state TEXT;
    invalid_result_state TEXT;
    invalid_entry_count BIGINT;
    invalid_entry_state TEXT;
    invalid_current_state TEXT;
    invalid_control_event_id UUID;
BEGIN
    -- Modern payload.from, when present and nonblank, is an assertion that
    -- must agree with the preceding immutable lifecycle result. Absent/null/
    -- blank remains the legacy encoding and is derived from history below.
    WITH lifecycle_base AS (
        SELECT lifecycle.subject_id AS work_item_id,
               lifecycle.id AS event_id,
               lifecycle.seq,
               lifecycle.kind,
               lifecycle.payload->>'from' AS payload_from,
               CASE
                   WHEN lifecycle.kind = 'work_item.created'
                   THEN COALESCE(NULLIF(lifecycle.payload->>'state', ''), 'captured')
                   ELSE lifecycle.payload->>'to'
               END AS result_state
        FROM events AS lifecycle
        JOIN work_item_assignment_state AS assignment_state
          ON assignment_state.work_item_id = lifecycle.subject_id
        WHERE lifecycle.subject_kind = 'work_item'
          AND lifecycle.kind IN ('work_item.created', 'work_item.transitioned')
    ), lifecycle_results AS (
        SELECT lifecycle_base.*,
               lag(result_state) OVER (
                   PARTITION BY work_item_id ORDER BY seq
               ) AS prior_state
        FROM lifecycle_base
    )
    SELECT work_item_id,
           event_id,
           payload_from,
           prior_state,
           result_state
    INTO invalid_work_item_id,
         invalid_event_id,
         invalid_payload_from,
         invalid_prior_state,
         invalid_result_state
    FROM lifecycle_results
    WHERE kind = 'work_item.transitioned'
      AND (
          prior_state IS NULL
          OR prior_state NOT IN (
              'captured', 'triaged', 'planned', 'awaiting_approval',
              'running', 'blocked', 'done', 'failed', 'canceled'
          )
          OR result_state IS NULL
          OR result_state NOT IN (
              'captured', 'triaged', 'planned', 'awaiting_approval',
              'running', 'blocked', 'done', 'failed', 'canceled'
          )
          OR (
              NULLIF(payload_from, '') IS NOT NULL
              AND payload_from IS DISTINCT FROM prior_state
          )
          OR (
              prior_state IN ('done', 'failed', 'canceled')
              AND result_state IS DISTINCT FROM prior_state
          )
      )
    ORDER BY work_item_id, seq
    LIMIT 1;

    IF invalid_work_item_id IS NOT NULL THEN
        RAISE EXCEPTION
            '0036 transition history mismatch for work_item % event %: payload_from=%, prior_state=%, result_state=%',
            invalid_work_item_id,
            invalid_event_id,
            invalid_payload_from,
            invalid_prior_state,
            invalid_result_state;
    END IF;

    WITH lifecycle_base AS (
        SELECT lifecycle.subject_id AS work_item_id,
               lifecycle.seq,
               CASE
                   WHEN lifecycle.kind = 'work_item.created'
                   THEN COALESCE(NULLIF(lifecycle.payload->>'state', ''), 'captured')
                   ELSE lifecycle.payload->>'to'
               END AS result_state
        FROM events AS lifecycle
        JOIN work_item_assignment_state AS assignment_state
          ON assignment_state.work_item_id = lifecycle.subject_id
        WHERE lifecycle.subject_kind = 'work_item'
          AND lifecycle.kind IN ('work_item.created', 'work_item.transitioned')
    ), lifecycle_results AS (
        SELECT lifecycle_base.*,
               lag(result_state) OVER (
                   PARTITION BY work_item_id ORDER BY seq
               ) AS prior_state
        FROM lifecycle_base
    ), entry_stats AS (
        SELECT work_item_id,
               count(*) AS entry_count,
               min(result_state) AS entry_state
        FROM lifecycle_results
        WHERE result_state IN ('done', 'failed', 'canceled')
          AND COALESCE(prior_state, '') NOT IN ('done', 'failed', 'canceled')
        GROUP BY work_item_id
    )
    SELECT assignment_state.work_item_id,
           COALESCE(entry_stats.entry_count, 0),
           entry_stats.entry_state,
           assignment_state.terminal_state
    INTO invalid_work_item_id,
         invalid_entry_count,
         invalid_entry_state,
         invalid_current_state
    FROM work_item_assignment_state AS assignment_state
    JOIN work_items AS work_item
      ON work_item.id = assignment_state.work_item_id
     AND work_item.state = assignment_state.terminal_state
    LEFT JOIN entry_stats
      ON entry_stats.work_item_id = assignment_state.work_item_id
    WHERE assignment_state.terminal_state IS NOT NULL
      AND (
          COALESCE(entry_stats.entry_count, 0) <> 1
          OR entry_stats.entry_state IS DISTINCT FROM assignment_state.terminal_state
      )
    ORDER BY assignment_state.work_item_id
    LIMIT 1;

    IF invalid_work_item_id IS NOT NULL THEN
        RAISE EXCEPTION
            '0036 terminal history invalid for work_item %: entry_count=%, entry_state=%, current_terminal_state=%',
            invalid_work_item_id,
            invalid_entry_count,
            invalid_entry_state,
            invalid_current_state;
    END IF;

    WITH lifecycle_base AS (
        SELECT lifecycle.subject_id AS work_item_id,
               lifecycle.id AS event_id,
               lifecycle.seq AS event_seq,
               CASE
                   WHEN lifecycle.kind = 'work_item.created'
                   THEN COALESCE(NULLIF(lifecycle.payload->>'state', ''), 'captured')
                   ELSE lifecycle.payload->>'to'
               END AS result_state
        FROM events AS lifecycle
        JOIN work_item_assignment_state AS assignment_state
          ON assignment_state.work_item_id = lifecycle.subject_id
        WHERE lifecycle.subject_kind = 'work_item'
          AND lifecycle.kind IN ('work_item.created', 'work_item.transitioned')
    ), lifecycle_results AS (
        SELECT lifecycle_base.*,
               lag(result_state) OVER (
                   PARTITION BY work_item_id ORDER BY event_seq
               ) AS prior_state
        FROM lifecycle_base
    ), terminal_entries AS (
        SELECT assignment_state.work_item_id,
               lifecycle_results.event_id,
               lifecycle_results.event_seq
        FROM work_item_assignment_state AS assignment_state
        JOIN lifecycle_results
          ON lifecycle_results.work_item_id = assignment_state.work_item_id
         AND lifecycle_results.result_state = assignment_state.terminal_state
         AND lifecycle_results.result_state IN ('done', 'failed', 'canceled')
         AND COALESCE(lifecycle_results.prior_state, '')
             NOT IN ('done', 'failed', 'canceled')
        WHERE assignment_state.terminal_state IS NOT NULL
    )
    SELECT terminal_entries.work_item_id,
           prior_control.id
    INTO invalid_work_item_id,
         invalid_control_event_id
    FROM terminal_entries
    JOIN LATERAL (
        SELECT control.id, control.kind, control.payload
        FROM events AS control
        WHERE control.subject_kind = 'work_item'
          AND control.subject_id = terminal_entries.work_item_id
          AND control.seq < terminal_entries.event_seq
          AND control.kind IN (
              'work_item.assigned',
              'work_item.assignment_released'
          )
        ORDER BY control.seq DESC
        LIMIT 1
    ) AS prior_control ON TRUE
    WHERE prior_control.kind = 'work_item.assigned'
      AND NULLIF(btrim(prior_control.payload->>'assignee_token_id'), '') IS NULL
    ORDER BY terminal_entries.work_item_id
    LIMIT 1;

    IF invalid_work_item_id IS NOT NULL THEN
        RAISE EXCEPTION
            '0036 terminal assignment history invalid for work_item %: assigned event % has missing assignee_token_id',
            invalid_work_item_id,
            invalid_control_event_id;
    END IF;
END
$$;

WITH lifecycle_base AS (
    SELECT lifecycle.subject_id AS work_item_id,
           lifecycle.id AS event_id,
           lifecycle.seq AS event_seq,
           lifecycle.occurred_at,
           CASE
               WHEN lifecycle.kind = 'work_item.created'
               THEN COALESCE(NULLIF(lifecycle.payload->>'state', ''), 'captured')
               ELSE lifecycle.payload->>'to'
           END AS result_state
    FROM events AS lifecycle
    JOIN work_item_assignment_state AS assignment_state
      ON assignment_state.work_item_id = lifecycle.subject_id
    WHERE lifecycle.subject_kind = 'work_item'
      AND lifecycle.kind IN ('work_item.created', 'work_item.transitioned')
), lifecycle_results AS (
    SELECT lifecycle_base.*,
           lag(result_state) OVER (
               PARTITION BY work_item_id ORDER BY event_seq
           ) AS prior_state
    FROM lifecycle_base
), terminal_entries AS (
    SELECT assignment_state.work_item_id,
           lifecycle_results.event_id,
           lifecycle_results.event_seq,
           lifecycle_results.occurred_at
    FROM work_item_assignment_state AS assignment_state
    JOIN lifecycle_results
      ON lifecycle_results.work_item_id = assignment_state.work_item_id
     AND lifecycle_results.result_state = assignment_state.terminal_state
     AND lifecycle_results.result_state IN ('done', 'failed', 'canceled')
     AND COALESCE(lifecycle_results.prior_state, '')
         NOT IN ('done', 'failed', 'canceled')
    WHERE assignment_state.terminal_state IS NOT NULL
), terminal_backfill AS (
    SELECT terminal_entries.*,
           CASE
               WHEN prior_control.kind = 'work_item.assigned'
               THEN (prior_control.payload->>'assignee_token_id')::UUID
           END AS addressee_token_id
    FROM terminal_entries
    LEFT JOIN LATERAL (
        SELECT control.kind, control.payload
        FROM events AS control
        WHERE control.subject_kind = 'work_item'
          AND control.subject_id = terminal_entries.work_item_id
          AND control.seq < terminal_entries.event_seq
          AND control.kind IN (
              'work_item.assigned',
              'work_item.assignment_released'
          )
        ORDER BY control.seq DESC
        LIMIT 1
    ) AS prior_control ON TRUE
)
UPDATE work_item_assignment_state AS assignment_state
SET terminal_addressee_token_id = terminal_backfill.addressee_token_id,
    state_event_id = terminal_backfill.event_id,
    state_event_seq = terminal_backfill.event_seq,
    updated_at = terminal_backfill.occurred_at
FROM terminal_backfill
WHERE assignment_state.work_item_id = terminal_backfill.work_item_id;

CREATE INDEX work_item_assignment_terminal_addressee_event_idx
    ON work_item_assignment_state (terminal_addressee_token_id, state_event_id)
    WHERE terminal_addressee_token_id IS NOT NULL;
