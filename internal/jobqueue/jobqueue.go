// Package jobqueue owns the durable Postgres-backed worker queue.
//
// Queue rows are caused by durable events, but lease state is operational
// coordination: pending rows can be rebuilt from dispatch.requested, while
// leased/done/failed/canceled state is how competing workers avoid duplicate
// execution at runtime.
package jobqueue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrInvalidLease   = errors.New("jobqueue: lease duration must be positive")
	ErrInvalidAttempt = errors.New("jobqueue: expected attempt must be positive")
)

const (
	reviewerCultivarRoot = "reviewer"
	reviewVerdictCheck   = "event:review.verdict_recorded"
)

// reduceDispatchJobsSQL closes inactive queue generations for one logical
// dispatch demand. Identity is the authoritative state-entry event preceding
// dispatch.requested in the same home-node event log. This also gives legacy
// payloads (which have no state_event_id) an exact identity without collapsing
// two state entries that happened in the same Unix second. An explicit
// state_event_id must agree with the derived predecessor or the row fails
// closed in reconciliation.
//
// $1 is KindDispatch. $2 optionally narrows the operation to one work item;
// pass nil to reconcile the complete queue. Unexpired leases are deliberately
// retained: they may already represent external work in flight. Claim paths
// park the replacement until that bounded lease completes or expires.
const reduceDispatchJobsSQL = `
	WITH ready AS MATERIALIZED (
		SELECT jq.id, jq.work_item_id, jq.state AS job_state, jq.lease_until
		FROM job_queue jq
		WHERE jq.kind = $1
		  AND ($2::uuid IS NULL OR jq.work_item_id = $2::uuid)
		  AND (
		    jq.state = 'pending'
		    OR (jq.state = 'leased' AND jq.lease_until <= now())
		  )
	),
	dispatch_generations AS (
		SELECT ready.id,
		       ready.job_state,
		       ready.lease_until,
		       ready.work_item_id,
		       demand.seq,
		       state_entry.id AS state_event_id
		FROM ready
		JOIN events demand
		  ON demand.id = ready.id
		 AND demand.subject_kind = 'work_item'
		 AND demand.subject_id = ready.work_item_id
		 AND demand.kind = 'dispatch.requested'
		JOIN LATERAL (
			SELECT lifecycle.id,
			       lifecycle.occurred_at,
			       CASE
			         WHEN lifecycle.kind = 'work_item.created'
			         THEN COALESCE(NULLIF(lifecycle.payload->>'state', ''), 'captured')
			         ELSE NULLIF(lifecycle.payload->>'to', '')
			       END AS state
			FROM events lifecycle
			WHERE lifecycle.subject_kind = demand.subject_kind
			  AND lifecycle.subject_id = demand.subject_id
			  AND lifecycle.kind IN ('work_item.created', 'work_item.transitioned')
			  AND lifecycle.seq < demand.seq
			  AND jsonb_typeof(lifecycle.payload) = 'object'
			  AND (
			    lifecycle.kind = 'work_item.created'
			    OR (
			      NULLIF(lifecycle.payload->>'to', '') IS NOT NULL
			      AND NULLIF(lifecycle.payload->>'to', '') IS DISTINCT FROM (
			        SELECT CASE
			                 WHEN prior.kind = 'work_item.created'
			                 THEN COALESCE(NULLIF(prior.payload->>'state', ''), 'captured')
			                 ELSE NULLIF(prior.payload->>'to', '')
			               END
			        FROM events prior
			        WHERE prior.subject_kind = lifecycle.subject_kind
			          AND prior.subject_id = lifecycle.subject_id
			          AND prior.kind IN ('work_item.created', 'work_item.transitioned')
			          AND prior.seq < lifecycle.seq
			          AND jsonb_typeof(prior.payload) = 'object'
			        ORDER BY prior.seq DESC
			        LIMIT 1
			      )
			    )
			  )
			ORDER BY lifecycle.seq DESC
			LIMIT 1
		) state_entry ON true
		WHERE jsonb_typeof(demand.payload) = 'object'
		  AND demand.payload->>'work_item_id' = ready.work_item_id::text
		  AND demand.payload->>'state' = state_entry.state
		  AND jsonb_typeof(demand.payload->'state_entered_at_unix') = 'number'
		  AND COALESCE(demand.payload->>'state_entered_at_unix', '') ~ '^-?[0-9]+$'
		  AND CASE
		        WHEN jsonb_typeof(demand.payload->'state_entered_at_unix') = 'number'
		         AND COALESCE(demand.payload->>'state_entered_at_unix', '') ~ '^-?[0-9]+$'
		        THEN (demand.payload->>'state_entered_at_unix')::numeric
		        ELSE NULL
		      END =
		      floor(extract(epoch FROM state_entry.occurred_at))
		  AND (
		    demand.payload->>'state_event_id' = state_entry.id::text
		    OR (
		      NOT (demand.payload ? 'state_event_id')
		      AND 1 = (
		        SELECT count(*)
		        FROM events possible_entry
		        WHERE possible_entry.subject_kind = demand.subject_kind
		          AND possible_entry.subject_id = demand.subject_id
		          AND possible_entry.seq < demand.seq
		          AND jsonb_typeof(possible_entry.payload) = 'object'
		          AND possible_entry.kind IN ('work_item.created', 'work_item.transitioned')
		          AND CASE
		                WHEN possible_entry.kind = 'work_item.created'
		                THEN COALESCE(NULLIF(possible_entry.payload->>'state', ''), 'captured')
		                ELSE NULLIF(possible_entry.payload->>'to', '')
		              END = demand.payload->>'state'
		          AND floor(extract(epoch FROM possible_entry.occurred_at)) =
		              CASE
		                WHEN jsonb_typeof(demand.payload->'state_entered_at_unix') = 'number'
		                 AND COALESCE(demand.payload->>'state_entered_at_unix', '') ~ '^-?[0-9]+$'
		                THEN (demand.payload->>'state_entered_at_unix')::numeric
		                ELSE NULL
		              END
		          AND (
		            possible_entry.kind = 'work_item.created'
		            OR NULLIF(possible_entry.payload->>'to', '') IS DISTINCT FROM (
		              SELECT CASE
		                       WHEN prior_entry.kind = 'work_item.created'
		                       THEN COALESCE(NULLIF(prior_entry.payload->>'state', ''), 'captured')
		                       ELSE NULLIF(prior_entry.payload->>'to', '')
		                     END
		              FROM events prior_entry
		              WHERE prior_entry.subject_kind = possible_entry.subject_kind
		                AND prior_entry.subject_id = possible_entry.subject_id
		                AND prior_entry.kind IN ('work_item.created', 'work_item.transitioned')
		                AND prior_entry.seq < possible_entry.seq
		                AND jsonb_typeof(prior_entry.payload) = 'object'
		              ORDER BY prior_entry.seq DESC
		              LIMIT 1
		            )
		          )
		      )
		    )
		  )
	),
	inactive AS (
		SELECT DISTINCT candidate.id
		FROM dispatch_generations candidate
		WHERE EXISTS (
			SELECT 1
			FROM events sibling
			JOIN LATERAL (
				SELECT lifecycle.id,
				       lifecycle.occurred_at,
				       CASE
				         WHEN lifecycle.kind = 'work_item.created'
				         THEN COALESCE(NULLIF(lifecycle.payload->>'state', ''), 'captured')
				         ELSE NULLIF(lifecycle.payload->>'to', '')
				       END AS state
				FROM events lifecycle
				WHERE lifecycle.subject_kind = sibling.subject_kind
				  AND lifecycle.subject_id = sibling.subject_id
				  AND lifecycle.kind IN ('work_item.created', 'work_item.transitioned')
				  AND lifecycle.seq < sibling.seq
				  AND jsonb_typeof(lifecycle.payload) = 'object'
				  AND (
				    lifecycle.kind = 'work_item.created'
				    OR (
				      NULLIF(lifecycle.payload->>'to', '') IS NOT NULL
				      AND NULLIF(lifecycle.payload->>'to', '') IS DISTINCT FROM (
				        SELECT CASE
				                 WHEN prior.kind = 'work_item.created'
				                 THEN COALESCE(NULLIF(prior.payload->>'state', ''), 'captured')
				                 ELSE NULLIF(prior.payload->>'to', '')
				               END
				        FROM events prior
				        WHERE prior.subject_kind = lifecycle.subject_kind
				          AND prior.subject_id = lifecycle.subject_id
				          AND prior.kind IN ('work_item.created', 'work_item.transitioned')
				          AND prior.seq < lifecycle.seq
				          AND jsonb_typeof(prior.payload) = 'object'
				        ORDER BY prior.seq DESC
				        LIMIT 1
				      )
				    )
				  )
				ORDER BY lifecycle.seq DESC
				LIMIT 1
			) sibling_entry ON true
			LEFT JOIN job_queue sibling_job ON sibling_job.id = sibling.id
			WHERE sibling.subject_kind = 'work_item'
			  AND sibling.subject_id = candidate.work_item_id
			  AND sibling.kind = 'dispatch.requested'
			  AND sibling_entry.id = candidate.state_event_id
			  AND jsonb_typeof(sibling.payload) = 'object'
			  AND sibling.payload->>'work_item_id' = candidate.work_item_id::text
			  AND sibling.payload->>'state' = sibling_entry.state
			  AND jsonb_typeof(sibling.payload->'state_entered_at_unix') = 'number'
			  AND COALESCE(sibling.payload->>'state_entered_at_unix', '') ~ '^-?[0-9]+$'
			  AND CASE
			        WHEN jsonb_typeof(sibling.payload->'state_entered_at_unix') = 'number'
			         AND COALESCE(sibling.payload->>'state_entered_at_unix', '') ~ '^-?[0-9]+$'
			        THEN (sibling.payload->>'state_entered_at_unix')::numeric
			        ELSE NULL
			      END =
			      floor(extract(epoch FROM sibling_entry.occurred_at))
			  AND (
			    sibling.payload->>'state_event_id' = sibling_entry.id::text
			    OR (
			      NOT (sibling.payload ? 'state_event_id')
			      AND 1 = (
			        SELECT count(*)
			        FROM events possible_entry
			        WHERE possible_entry.subject_kind = sibling.subject_kind
			          AND possible_entry.subject_id = sibling.subject_id
			          AND possible_entry.seq < sibling.seq
			          AND jsonb_typeof(possible_entry.payload) = 'object'
			          AND possible_entry.kind IN ('work_item.created', 'work_item.transitioned')
			          AND CASE
			                WHEN possible_entry.kind = 'work_item.created'
			                THEN COALESCE(NULLIF(possible_entry.payload->>'state', ''), 'captured')
			                ELSE NULLIF(possible_entry.payload->>'to', '')
			              END = sibling.payload->>'state'
			          AND floor(extract(epoch FROM possible_entry.occurred_at)) =
			              CASE
			                WHEN jsonb_typeof(sibling.payload->'state_entered_at_unix') = 'number'
			                 AND COALESCE(sibling.payload->>'state_entered_at_unix', '') ~ '^-?[0-9]+$'
			                THEN (sibling.payload->>'state_entered_at_unix')::numeric
			                ELSE NULL
			              END
			          AND (
			            possible_entry.kind = 'work_item.created'
			            OR NULLIF(possible_entry.payload->>'to', '') IS DISTINCT FROM (
			              SELECT CASE
			                       WHEN prior_entry.kind = 'work_item.created'
			                       THEN COALESCE(NULLIF(prior_entry.payload->>'state', ''), 'captured')
			                       ELSE NULLIF(prior_entry.payload->>'to', '')
			                     END
			              FROM events prior_entry
			              WHERE prior_entry.subject_kind = possible_entry.subject_kind
			                AND prior_entry.subject_id = possible_entry.subject_id
			                AND prior_entry.kind IN ('work_item.created', 'work_item.transitioned')
			                AND prior_entry.seq < possible_entry.seq
			                AND jsonb_typeof(prior_entry.payload) = 'object'
			              ORDER BY prior_entry.seq DESC
			              LIMIT 1
			            )
			          )
			      )
			    )
			  )
			  AND (
			    sibling.seq > candidate.seq
			    OR sibling_job.state = 'done'
			  )
		)
		OR EXISTS (
			SELECT 1
			FROM events newer_raw
			WHERE newer_raw.subject_kind = 'work_item'
			  AND newer_raw.subject_id = candidate.work_item_id
			  AND newer_raw.kind = 'dispatch.requested'
			  AND newer_raw.seq > candidate.seq
		)
	)
	UPDATE job_queue jq
	SET state = 'canceled',
	    lease_until = NULL,
	    updated_at = now()
	FROM inactive
	WHERE jq.id = inactive.id
	  AND (
	    jq.state = 'pending'
	    OR (jq.state = 'leased' AND jq.lease_until <= now())
	  )
`

// claimableDispatchGenerationSQL is appended to claim queries whose candidate
// row is aliased jq. It rejects an older event generation, a logical demand
// already completed by any generation, and a replacement parked behind an
// older live lease. Failed and canceled siblings do not satisfy the demand.
const claimableDispatchGenerationSQL = `
			  AND EXISTS (
			    SELECT 1
			    FROM events demand
			    JOIN LATERAL (
			      SELECT lifecycle.id,
			             lifecycle.occurred_at,
			             CASE
			               WHEN lifecycle.kind = 'work_item.created'
			               THEN COALESCE(NULLIF(lifecycle.payload->>'state', ''), 'captured')
			               ELSE NULLIF(lifecycle.payload->>'to', '')
			             END AS state
			      FROM events lifecycle
			      WHERE lifecycle.subject_kind = demand.subject_kind
			        AND lifecycle.subject_id = demand.subject_id
			        AND lifecycle.kind IN ('work_item.created', 'work_item.transitioned')
			        AND lifecycle.seq < demand.seq
			        AND jsonb_typeof(lifecycle.payload) = 'object'
			        AND (
			          lifecycle.kind = 'work_item.created'
			          OR (
			            NULLIF(lifecycle.payload->>'to', '') IS NOT NULL
			            AND NULLIF(lifecycle.payload->>'to', '') IS DISTINCT FROM (
			              SELECT CASE
			                       WHEN prior.kind = 'work_item.created'
			                       THEN COALESCE(NULLIF(prior.payload->>'state', ''), 'captured')
			                       ELSE NULLIF(prior.payload->>'to', '')
			                     END
			              FROM events prior
			              WHERE prior.subject_kind = lifecycle.subject_kind
			                AND prior.subject_id = lifecycle.subject_id
			                AND prior.kind IN ('work_item.created', 'work_item.transitioned')
			                AND prior.seq < lifecycle.seq
			                AND jsonb_typeof(prior.payload) = 'object'
			              ORDER BY prior.seq DESC
			              LIMIT 1
			            )
			          )
			        )
			      ORDER BY lifecycle.seq DESC
			      LIMIT 1
			    ) state_entry ON true
			    JOIN LATERAL (
			      SELECT lifecycle.id,
			             lifecycle.occurred_at,
			             CASE
			               WHEN lifecycle.kind = 'work_item.created'
			               THEN COALESCE(NULLIF(lifecycle.payload->>'state', ''), 'captured')
			               ELSE NULLIF(lifecycle.payload->>'to', '')
			             END AS state
			      FROM events lifecycle
			      WHERE lifecycle.subject_kind = demand.subject_kind
			        AND lifecycle.subject_id = demand.subject_id
			        AND lifecycle.kind IN ('work_item.created', 'work_item.transitioned')
			        AND jsonb_typeof(lifecycle.payload) = 'object'
			        AND (
			          lifecycle.kind = 'work_item.created'
			          OR (
			            NULLIF(lifecycle.payload->>'to', '') IS NOT NULL
			            AND NULLIF(lifecycle.payload->>'to', '') IS DISTINCT FROM (
			              SELECT CASE
			                       WHEN prior.kind = 'work_item.created'
			                       THEN COALESCE(NULLIF(prior.payload->>'state', ''), 'captured')
			                       ELSE NULLIF(prior.payload->>'to', '')
			                     END
			              FROM events prior
			              WHERE prior.subject_kind = lifecycle.subject_kind
			                AND prior.subject_id = lifecycle.subject_id
			                AND prior.kind IN ('work_item.created', 'work_item.transitioned')
			                AND prior.seq < lifecycle.seq
			                AND jsonb_typeof(prior.payload) = 'object'
			              ORDER BY prior.seq DESC
			              LIMIT 1
			            )
			          )
			        )
			      ORDER BY lifecycle.seq DESC
			      LIMIT 1
			    ) current_entry ON true
			    WHERE demand.id = jq.id
			      AND demand.subject_kind = 'work_item'
			      AND demand.subject_id = jq.work_item_id
			      AND demand.kind = 'dispatch.requested'
			      AND jsonb_typeof(demand.payload) = 'object'
			      AND jq.payload = demand.payload
			      AND demand.payload->>'work_item_id' = jq.work_item_id::text
			      AND demand.payload->>'state' = state_entry.state
			      AND state_entry.id = current_entry.id
			      AND current_entry.state = wi.state
			      AND current_entry.occurred_at = wi.state_entered_at
			      AND jsonb_typeof(demand.payload->'state_entered_at_unix') = 'number'
			      AND COALESCE(demand.payload->>'state_entered_at_unix', '') ~ '^-?[0-9]+$'
			      AND CASE
			            WHEN jsonb_typeof(demand.payload->'state_entered_at_unix') = 'number'
			             AND COALESCE(demand.payload->>'state_entered_at_unix', '') ~ '^-?[0-9]+$'
			            THEN (demand.payload->>'state_entered_at_unix')::numeric
			            ELSE NULL
			          END =
			          floor(extract(epoch FROM state_entry.occurred_at))
			      AND (
			        demand.payload->>'state_event_id' = state_entry.id::text
			        OR (
			          NOT (demand.payload ? 'state_event_id')
			          AND 1 = (
			            SELECT count(*)
			            FROM events possible_entry
			            WHERE possible_entry.subject_kind = demand.subject_kind
			              AND possible_entry.subject_id = demand.subject_id
			              AND possible_entry.seq < demand.seq
			              AND jsonb_typeof(possible_entry.payload) = 'object'
			              AND possible_entry.kind IN ('work_item.created', 'work_item.transitioned')
			              AND CASE
			                    WHEN possible_entry.kind = 'work_item.created'
			                    THEN COALESCE(NULLIF(possible_entry.payload->>'state', ''), 'captured')
			                    ELSE NULLIF(possible_entry.payload->>'to', '')
			                  END = demand.payload->>'state'
			              AND floor(extract(epoch FROM possible_entry.occurred_at)) =
			                  CASE
			                    WHEN jsonb_typeof(demand.payload->'state_entered_at_unix') = 'number'
			                     AND COALESCE(demand.payload->>'state_entered_at_unix', '') ~ '^-?[0-9]+$'
			                    THEN (demand.payload->>'state_entered_at_unix')::numeric
			                    ELSE NULL
			                  END
			              AND (
			                possible_entry.kind = 'work_item.created'
			                OR NULLIF(possible_entry.payload->>'to', '') IS DISTINCT FROM (
			                  SELECT CASE
			                           WHEN prior_entry.kind = 'work_item.created'
			                           THEN COALESCE(NULLIF(prior_entry.payload->>'state', ''), 'captured')
			                           ELSE NULLIF(prior_entry.payload->>'to', '')
			                         END
			                  FROM events prior_entry
			                  WHERE prior_entry.subject_kind = possible_entry.subject_kind
			                    AND prior_entry.subject_id = possible_entry.subject_id
			                    AND prior_entry.kind IN ('work_item.created', 'work_item.transitioned')
			                    AND prior_entry.seq < possible_entry.seq
			                    AND jsonb_typeof(prior_entry.payload) = 'object'
			                  ORDER BY prior_entry.seq DESC
			                  LIMIT 1
			                )
			              )
			          )
			        )
			      )
			      AND NOT EXISTS (
			        SELECT 1
			        FROM events newer_raw
			        WHERE newer_raw.subject_kind = demand.subject_kind
			          AND newer_raw.subject_id = demand.subject_id
			          AND newer_raw.kind = demand.kind
			          AND newer_raw.seq > demand.seq
			      )
			      AND NOT EXISTS (
			        SELECT 1
			        FROM events older_raw
			        JOIN job_queue older_job ON older_job.id = older_raw.id
			        WHERE older_raw.subject_kind = demand.subject_kind
			          AND older_raw.subject_id = demand.subject_id
			          AND older_raw.kind = demand.kind
			          AND older_raw.seq < demand.seq
			          AND older_job.state = 'leased'
			          AND older_job.lease_until > clock_timestamp()
			      )
			      AND NOT EXISTS (
			        SELECT 1
			        FROM events sibling
			        JOIN LATERAL (
			          SELECT lifecycle.id,
			                 lifecycle.occurred_at,
			                 CASE
			                   WHEN lifecycle.kind = 'work_item.created'
			                   THEN COALESCE(NULLIF(lifecycle.payload->>'state', ''), 'captured')
			                   ELSE NULLIF(lifecycle.payload->>'to', '')
			                 END AS state
			          FROM events lifecycle
			          WHERE lifecycle.subject_kind = sibling.subject_kind
			            AND lifecycle.subject_id = sibling.subject_id
			            AND lifecycle.kind IN ('work_item.created', 'work_item.transitioned')
			            AND lifecycle.seq < sibling.seq
			            AND jsonb_typeof(lifecycle.payload) = 'object'
			            AND (
			              lifecycle.kind = 'work_item.created'
			              OR (
			                NULLIF(lifecycle.payload->>'to', '') IS NOT NULL
			                AND NULLIF(lifecycle.payload->>'to', '') IS DISTINCT FROM (
			                  SELECT CASE
			                           WHEN prior.kind = 'work_item.created'
			                           THEN COALESCE(NULLIF(prior.payload->>'state', ''), 'captured')
			                           ELSE NULLIF(prior.payload->>'to', '')
			                         END
			                  FROM events prior
			                  WHERE prior.subject_kind = lifecycle.subject_kind
			                    AND prior.subject_id = lifecycle.subject_id
			                    AND prior.kind IN ('work_item.created', 'work_item.transitioned')
			                    AND prior.seq < lifecycle.seq
			                    AND jsonb_typeof(prior.payload) = 'object'
			                  ORDER BY prior.seq DESC
			                  LIMIT 1
			                )
			              )
			            )
			          ORDER BY lifecycle.seq DESC
			          LIMIT 1
			        ) sibling_entry ON true
			        LEFT JOIN job_queue sibling_job ON sibling_job.id = sibling.id
			        WHERE sibling.subject_kind = demand.subject_kind
			          AND sibling.subject_id = demand.subject_id
			          AND sibling.kind = demand.kind
			          AND sibling_entry.id = state_entry.id
			          AND jsonb_typeof(sibling.payload) = 'object'
			          AND sibling.payload->>'work_item_id' = jq.work_item_id::text
			          AND sibling.payload->>'state' = sibling_entry.state
			          AND jsonb_typeof(sibling.payload->'state_entered_at_unix') = 'number'
			          AND COALESCE(sibling.payload->>'state_entered_at_unix', '') ~ '^-?[0-9]+$'
			          AND CASE
			                WHEN jsonb_typeof(sibling.payload->'state_entered_at_unix') = 'number'
			                 AND COALESCE(sibling.payload->>'state_entered_at_unix', '') ~ '^-?[0-9]+$'
			                THEN (sibling.payload->>'state_entered_at_unix')::numeric
			                ELSE NULL
			              END =
			              floor(extract(epoch FROM sibling_entry.occurred_at))
			          AND (
			            sibling.payload->>'state_event_id' = sibling_entry.id::text
			            OR (
			              NOT (sibling.payload ? 'state_event_id')
			              AND 1 = (
			                SELECT count(*)
			                FROM events possible_entry
			                WHERE possible_entry.subject_kind = sibling.subject_kind
			                  AND possible_entry.subject_id = sibling.subject_id
			                  AND possible_entry.seq < sibling.seq
			                  AND jsonb_typeof(possible_entry.payload) = 'object'
			                  AND possible_entry.kind IN ('work_item.created', 'work_item.transitioned')
			                  AND CASE
			                        WHEN possible_entry.kind = 'work_item.created'
			                        THEN COALESCE(NULLIF(possible_entry.payload->>'state', ''), 'captured')
			                        ELSE NULLIF(possible_entry.payload->>'to', '')
			                      END = sibling.payload->>'state'
			                  AND floor(extract(epoch FROM possible_entry.occurred_at)) =
			                      CASE
			                        WHEN jsonb_typeof(sibling.payload->'state_entered_at_unix') = 'number'
			                         AND COALESCE(sibling.payload->>'state_entered_at_unix', '') ~ '^-?[0-9]+$'
			                        THEN (sibling.payload->>'state_entered_at_unix')::numeric
			                        ELSE NULL
			                      END
			                  AND (
			                    possible_entry.kind = 'work_item.created'
			                    OR NULLIF(possible_entry.payload->>'to', '') IS DISTINCT FROM (
			                      SELECT CASE
			                               WHEN prior_entry.kind = 'work_item.created'
			                               THEN COALESCE(NULLIF(prior_entry.payload->>'state', ''), 'captured')
			                               ELSE NULLIF(prior_entry.payload->>'to', '')
			                             END
			                      FROM events prior_entry
			                      WHERE prior_entry.subject_kind = possible_entry.subject_kind
			                        AND prior_entry.subject_id = possible_entry.subject_id
			                        AND prior_entry.kind IN ('work_item.created', 'work_item.transitioned')
			                        AND prior_entry.seq < possible_entry.seq
			                        AND jsonb_typeof(prior_entry.payload) = 'object'
			                      ORDER BY prior_entry.seq DESC
			                      LIMIT 1
			                    )
			                  )
			              )
			            )
			          )
			          AND (
			            sibling.seq > demand.seq
			            OR sibling_job.state = 'done'
			            OR (
			              sibling.seq < demand.seq
			              AND sibling_job.state = 'leased'
			              AND sibling_job.lease_until > clock_timestamp()
			            )
			          )
			      )
			  )
`

// staleDispatchJobsSQL validates only ready rows and derives lifecycle identity
// from the immutable log. Gated rows keep a valid older epoch dormant, but a
// malformed event/job pair is always canceled. The final update repeats the
// ready predicate so a lease acquired after the CTE snapshot is never canceled.
const staleDispatchJobsSQL = `
	WITH ready AS MATERIALIZED (
		SELECT jq.id, jq.work_item_id, jq.payload
		FROM job_queue jq
		WHERE jq.kind = $1
		  AND (
		    jq.state = 'pending'
		    OR (jq.state = 'leased' AND jq.lease_until <= now())
		  )
	),
	validated AS (
		SELECT ready.*,
		       wi.id AS current_work_item_id,
		       wi.state AS projected_state,
		       wi.state_entered_at AS projected_state_entered_at,
		       wi.human_review_status,
		       wi.suggested_convergence_checks,
		       demand.id AS demand_id,
		       demand.seq AS demand_seq,
		       demand.payload AS demand_payload,
		       state_entry.id AS state_event_id,
		       state_entry.state AS state_entry_state,
		       state_entry.occurred_at AS state_entered_at,
		       current_entry.id AS current_state_event_id,
		       current_entry.state AS current_event_state,
		       current_entry.occurred_at AS current_event_entered_at,
		       EXISTS (
		         SELECT 1
		         FROM events created
		         WHERE created.subject_kind = 'work_item'
		           AND created.subject_id = ready.work_item_id
		           AND created.kind = 'work_item.created'
		           AND split_part(btrim(COALESCE(created.payload->>'cultivar', '')), '@', 1) = $2
		       ) AS created_as_reviewer
		FROM ready
		LEFT JOIN work_items wi ON wi.id = ready.work_item_id
		LEFT JOIN events demand
		  ON demand.id = ready.id
		 AND demand.subject_kind = 'work_item'
		 AND demand.subject_id = ready.work_item_id
		 AND demand.kind = 'dispatch.requested'
		LEFT JOIN LATERAL (
			SELECT lifecycle.id,
			       lifecycle.occurred_at,
			       CASE
			         WHEN lifecycle.kind = 'work_item.created'
			         THEN COALESCE(NULLIF(lifecycle.payload->>'state', ''), 'captured')
			         ELSE NULLIF(lifecycle.payload->>'to', '')
			       END AS state
			FROM events lifecycle
			WHERE lifecycle.subject_kind = demand.subject_kind
			  AND lifecycle.subject_id = demand.subject_id
			  AND lifecycle.kind IN ('work_item.created', 'work_item.transitioned')
			  AND lifecycle.seq < demand.seq
			  AND jsonb_typeof(lifecycle.payload) = 'object'
			  AND (
			    lifecycle.kind = 'work_item.created'
			    OR (
			      NULLIF(lifecycle.payload->>'to', '') IS NOT NULL
			      AND NULLIF(lifecycle.payload->>'to', '') IS DISTINCT FROM (
			        SELECT CASE
			                 WHEN prior.kind = 'work_item.created'
			                 THEN COALESCE(NULLIF(prior.payload->>'state', ''), 'captured')
			                 ELSE NULLIF(prior.payload->>'to', '')
			               END
			        FROM events prior
			        WHERE prior.subject_kind = lifecycle.subject_kind
			          AND prior.subject_id = lifecycle.subject_id
			          AND prior.kind IN ('work_item.created', 'work_item.transitioned')
			          AND prior.seq < lifecycle.seq
			          AND jsonb_typeof(prior.payload) = 'object'
			        ORDER BY prior.seq DESC
			        LIMIT 1
			      )
			    )
			  )
			ORDER BY lifecycle.seq DESC
			LIMIT 1
		) state_entry ON true
		LEFT JOIN LATERAL (
			SELECT lifecycle.id,
			       lifecycle.occurred_at,
			       CASE
			         WHEN lifecycle.kind = 'work_item.created'
			         THEN COALESCE(NULLIF(lifecycle.payload->>'state', ''), 'captured')
			         ELSE NULLIF(lifecycle.payload->>'to', '')
			       END AS state
			FROM events lifecycle
			WHERE lifecycle.subject_kind = 'work_item'
			  AND lifecycle.subject_id = wi.id
			  AND lifecycle.kind IN ('work_item.created', 'work_item.transitioned')
			  AND jsonb_typeof(lifecycle.payload) = 'object'
			  AND (
			    lifecycle.kind = 'work_item.created'
			    OR (
			      NULLIF(lifecycle.payload->>'to', '') IS NOT NULL
			      AND NULLIF(lifecycle.payload->>'to', '') IS DISTINCT FROM (
			        SELECT CASE
			                 WHEN prior.kind = 'work_item.created'
			                 THEN COALESCE(NULLIF(prior.payload->>'state', ''), 'captured')
			                 ELSE NULLIF(prior.payload->>'to', '')
			               END
			        FROM events prior
			        WHERE prior.subject_kind = lifecycle.subject_kind
			          AND prior.subject_id = lifecycle.subject_id
			          AND prior.kind IN ('work_item.created', 'work_item.transitioned')
			          AND prior.seq < lifecycle.seq
			          AND jsonb_typeof(prior.payload) = 'object'
			        ORDER BY prior.seq DESC
			        LIMIT 1
			      )
			    )
			  )
			ORDER BY lifecycle.seq DESC
			LIMIT 1
		) current_entry ON true
	),
	stale AS (
		SELECT candidate.id
		FROM validated candidate
		WHERE current_work_item_id IS NULL
		   OR projected_state IN ('done', 'failed', 'canceled')
		   OR demand_id IS NULL
		   OR jsonb_typeof(payload) IS DISTINCT FROM 'object'
		   OR payload IS DISTINCT FROM demand_payload
		   OR payload->>'work_item_id' IS DISTINCT FROM work_item_id::text
		   OR state_event_id IS NULL
		   OR payload->>'state' IS DISTINCT FROM state_entry_state
		   OR btrim(COALESCE(payload->>'cultivar', '')) = ''
		   OR jsonb_typeof(payload->'state_entered_at_unix') IS DISTINCT FROM 'number'
		   OR CASE
		        WHEN jsonb_typeof(payload->'state_entered_at_unix') = 'number'
		         AND COALESCE(payload->>'state_entered_at_unix', '') ~ '^-?[0-9]+$'
		        THEN (payload->>'state_entered_at_unix')::numeric <>
		             floor(extract(epoch FROM state_entered_at))
		        ELSE true
		      END
		   OR (
		        payload ? 'state_event_id'
		        AND payload->>'state_event_id' IS DISTINCT FROM state_event_id::text
		      )
		   OR (
		        NOT (candidate.payload ? 'state_event_id')
		        AND 1 <> (
		          SELECT count(*)
		          FROM events possible_entry
		          WHERE possible_entry.subject_kind = 'work_item'
		            AND possible_entry.subject_id = candidate.work_item_id
		            AND possible_entry.seq < candidate.demand_seq
		            AND jsonb_typeof(possible_entry.payload) = 'object'
		            AND possible_entry.kind IN ('work_item.created', 'work_item.transitioned')
		            AND CASE
		                  WHEN possible_entry.kind = 'work_item.created'
		                  THEN COALESCE(NULLIF(possible_entry.payload->>'state', ''), 'captured')
		                  ELSE NULLIF(possible_entry.payload->>'to', '')
		                END = candidate.payload->>'state'
		            AND floor(extract(epoch FROM possible_entry.occurred_at)) =
		                CASE
		                  WHEN jsonb_typeof(candidate.payload->'state_entered_at_unix') = 'number'
		                   AND COALESCE(candidate.payload->>'state_entered_at_unix', '') ~ '^-?[0-9]+$'
		                  THEN (candidate.payload->>'state_entered_at_unix')::numeric
		                  ELSE NULL
		                END
		            AND (
		              possible_entry.kind = 'work_item.created'
		              OR NULLIF(possible_entry.payload->>'to', '') IS DISTINCT FROM (
		                SELECT CASE
		                         WHEN prior_entry.kind = 'work_item.created'
		                         THEN COALESCE(NULLIF(prior_entry.payload->>'state', ''), 'captured')
		                         ELSE NULLIF(prior_entry.payload->>'to', '')
		                       END
		                FROM events prior_entry
		                WHERE prior_entry.subject_kind = possible_entry.subject_kind
		                  AND prior_entry.subject_id = possible_entry.subject_id
		                  AND prior_entry.kind IN ('work_item.created', 'work_item.transitioned')
		                  AND prior_entry.seq < possible_entry.seq
		                  AND jsonb_typeof(prior_entry.payload) = 'object'
		                ORDER BY prior_entry.seq DESC
		                LIMIT 1
		              )
		            )
		        )
		      )
		   OR (
		     projected_state <> 'blocked'
		     AND human_review_status <> 'blocked'
		     AND (
		       state_event_id IS DISTINCT FROM current_state_event_id
		       OR current_event_state IS DISTINCT FROM projected_state
		       OR current_event_entered_at IS DISTINCT FROM projected_state_entered_at
		       OR jsonb_array_length(suggested_convergence_checks) = 0
		       OR (
		         (
		           split_part(btrim(COALESCE(candidate.payload->>'cultivar', '')), '@', 1) = $2
		           OR created_as_reviewer
		         )
		         AND (
		           split_part(btrim(COALESCE(candidate.payload->>'cultivar', '')), '@', 1) <> $2
		           OR NOT created_as_reviewer
		           OR NOT (suggested_convergence_checks ? $3)
		         )
		       )
		     )
		   )
	)
	UPDATE job_queue jq
	SET state = 'canceled',
	    lease_until = NULL,
	    updated_at = now()
	FROM stale
	WHERE jq.id = stale.id
	  AND (
	    jq.state = 'pending'
	    OR (jq.state = 'leased' AND jq.lease_until <= now())
	  )
`

type JobState string

const (
	JobPending  JobState = "pending"
	JobLeased   JobState = "leased"
	JobDone     JobState = "done"
	JobFailed   JobState = "failed"
	JobCanceled JobState = "canceled"
)

type Job struct {
	ID         uuid.UUID
	Kind       string
	WorkItemID uuid.UUID
	State      JobState
	Payload    json.RawMessage
	Attempts   int
	LeaseUntil *time.Time
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

type Service struct {
	pool *pgxpool.Pool
}

func NewService(pool *pgxpool.Pool) *Service {
	return &Service{pool: pool}
}

// ReconcileDispatchJobs cancels dispatch jobs that can no longer represent a
// runnable state epoch. Queue state is operational coordination (not a durable
// domain projection), so this is intentionally a direct update just like lease
// and terminal-state changes.
//
// Human-review-blocked and lifecycle-blocked rows are deliberately excluded
// from state-validity cancellation: the newest generation remains pending and
// dormant so removing the gate can make that epoch claimable. Superseded
// generations are canceled even while the newest is gated. Unknown job kinds
// and otherwise-valid non-review dispatches are left untouched.
func (s *Service) ReconcileDispatchJobs(ctx context.Context) (int, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, fmt.Errorf("jobqueue: begin dispatch reconciliation: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// A worker may lease a job just before a gate appears. Once that bounded
	// lease expires, normalize it back to pending so its dormant state is
	// explicit and removing the gate can make the same job claimable again.
	if _, err := tx.Exec(ctx, `
		UPDATE job_queue jq
		SET state = 'pending',
		    lease_until = NULL,
		    updated_at = now()
		FROM work_items wi
		WHERE jq.kind = $1
		  AND jq.work_item_id = wi.id
		  AND jq.state = 'leased'
		  AND jq.lease_until <= now()
		  AND wi.state NOT IN ('done', 'failed', 'canceled')
		  AND (
		    wi.state = 'blocked'
		    OR wi.human_review_status = 'blocked'
		  )
	`, KindDispatch); err != nil {
		return 0, fmt.Errorf("jobqueue: release dormant dispatch leases: %w", err)
	}

	reducedTag, err := tx.Exec(ctx, reduceDispatchJobsSQL, KindDispatch, nil)
	if err != nil {
		return 0, fmt.Errorf("jobqueue: reduce dispatch jobs: %w", err)
	}

	tag, err := tx.Exec(ctx, staleDispatchJobsSQL, KindDispatch, reviewerCultivarRoot, reviewVerdictCheck)
	if err != nil {
		return 0, fmt.Errorf("jobqueue: reconcile dispatch jobs: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("jobqueue: commit dispatch reconciliation: %w", err)
	}
	return int(reducedTag.RowsAffected() + tag.RowsAffected()), nil
}

// ClaimNextReview leases one ready reviewer dispatch. The queue may also hold
// ordinary checklist-worker dispatches; production review automation must not
// claim those and thereby pretend the deterministic reconciler executed agent
// work. Reviewer identity is cross-checked against both the dispatch payload
// and the work item's event-backed launch metadata.
func (s *Service) ClaimNextReview(ctx context.Context, lease time.Duration) (Job, bool, error) {
	if lease <= 0 {
		return Job{}, false, ErrInvalidLease
	}
	leaseMillis := lease.Milliseconds()
	if leaseMillis <= 0 {
		leaseMillis = 1
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Job{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	job, found, err := claimNextReviewInTx(ctx, tx, leaseMillis)
	if err != nil {
		return Job{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Job{}, false, err
	}
	return job, found, nil
}

func claimNextReviewInTx(ctx context.Context, tx pgx.Tx, leaseMillis int64) (Job, bool, error) {
	row := tx.QueryRow(ctx, `
		WITH candidate AS (
			SELECT jq.id
			FROM job_queue jq
			JOIN work_items wi ON wi.id = jq.work_item_id
			WHERE jq.kind = $2
			  AND (
			    jq.state = 'pending'
			    OR (jq.state = 'leased' AND jq.lease_until <= now())
			  )
			  AND wi.state IN ('captured', 'triaged', 'planned')
			  AND wi.human_review_status <> 'blocked'
			  AND jsonb_array_length(wi.suggested_convergence_checks) > 0
			  AND wi.suggested_convergence_checks ? $4
			  AND jsonb_typeof(jq.payload) = 'object'
			  AND jq.payload->>'work_item_id' = jq.work_item_id::text
			  AND jq.payload->>'state' = wi.state
			  AND jsonb_typeof(jq.payload->'state_entered_at_unix') = 'number'
			  AND COALESCE(jq.payload->>'state_entered_at_unix', '') ~ '^-?[0-9]+$'
			  AND floor(extract(epoch FROM wi.state_entered_at)) =
			      CASE
			        WHEN jsonb_typeof(jq.payload->'state_entered_at_unix') = 'number'
			         AND COALESCE(jq.payload->>'state_entered_at_unix', '') ~ '^-?[0-9]+$'
			        THEN (jq.payload->>'state_entered_at_unix')::numeric
			        ELSE NULL
			      END
			  AND split_part(btrim(COALESCE(jq.payload->>'cultivar', '')), '@', 1) = $3
			  AND EXISTS (
			    SELECT 1
			    FROM events created
			    WHERE created.subject_kind = 'work_item'
			      AND created.subject_id = wi.id
			      AND created.kind = 'work_item.created'
			      AND split_part(btrim(COALESCE(created.payload->>'cultivar', '')), '@', 1) = $3
			  )
	`+claimableDispatchGenerationSQL+`
			ORDER BY jq.created_at ASC, jq.id ASC
			FOR UPDATE OF jq SKIP LOCKED
			LIMIT 1
		)
		UPDATE job_queue jq
		SET state = 'leased',
		    attempts = attempts + 1,
		    lease_until = clock_timestamp() + ($1::bigint * interval '1 millisecond'),
		    updated_at = clock_timestamp()
		FROM candidate
		WHERE jq.id = candidate.id
		RETURNING jq.id, jq.kind, jq.work_item_id, jq.state, jq.payload,
		          jq.attempts, jq.lease_until, jq.created_at, jq.updated_at
	`, leaseMillis, KindDispatch, reviewerCultivarRoot, reviewVerdictCheck)
	job, err := scanJob(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Job{}, false, nil
		}
		return Job{}, false, fmt.Errorf("jobqueue: claim next review: %w", err)
	}
	return job, true, nil
}

// ClaimNext leases one ready job using SELECT ... FOR UPDATE SKIP LOCKED.
// Competing callers skip rows already locked by another transaction and
// therefore claim disjoint jobs without any process-local coordination.
func (s *Service) ClaimNext(ctx context.Context, lease time.Duration) (Job, bool, error) {
	if lease <= 0 {
		return Job{}, false, ErrInvalidLease
	}
	leaseMillis := lease.Milliseconds()
	if leaseMillis <= 0 {
		leaseMillis = 1
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Job{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	job, found, err := claimNextInTx(ctx, tx, leaseMillis)
	if err != nil {
		return Job{}, false, err
	}
	if !found {
		if err := tx.Commit(ctx); err != nil {
			return Job{}, false, err
		}
		return Job{}, false, nil
	}
	if err := tx.Commit(ctx); err != nil {
		return Job{}, false, err
	}
	return job, true, nil
}

func claimNextInTx(ctx context.Context, tx pgx.Tx, leaseMillis int64) (Job, bool, error) {
	row := tx.QueryRow(ctx, `
		WITH candidate AS (
			SELECT jq.id
			FROM job_queue jq
			JOIN work_items wi ON wi.id = jq.work_item_id
			WHERE (
			    jq.state = 'pending'
			    OR (jq.state = 'leased' AND jq.lease_until <= now())
			  )
			  AND (
			    jq.kind <> $2
			    OR (
			      wi.state IN ('captured', 'triaged', 'planned')
			      AND wi.human_review_status <> 'blocked'
			      AND jsonb_array_length(wi.suggested_convergence_checks) > 0
			      AND jsonb_typeof(jq.payload) = 'object'
			      AND jq.payload->>'work_item_id' = jq.work_item_id::text
			      AND jq.payload->>'state' = wi.state
			      AND btrim(COALESCE(jq.payload->>'cultivar', '')) <> ''
			      AND jsonb_typeof(jq.payload->'state_entered_at_unix') = 'number'
			      AND COALESCE(jq.payload->>'state_entered_at_unix', '') ~ '^-?[0-9]+$'
			      AND floor(extract(epoch FROM wi.state_entered_at)) =
			          CASE
			            WHEN jsonb_typeof(jq.payload->'state_entered_at_unix') = 'number'
			             AND COALESCE(jq.payload->>'state_entered_at_unix', '') ~ '^-?[0-9]+$'
			            THEN (jq.payload->>'state_entered_at_unix')::numeric
			            ELSE NULL
			          END
	`+claimableDispatchGenerationSQL+`
			    )
			  )
			ORDER BY jq.created_at ASC, jq.id ASC
			FOR UPDATE OF jq SKIP LOCKED
			LIMIT 1
		)
		UPDATE job_queue jq
		SET state = 'leased',
		    attempts = attempts + 1,
		    lease_until = clock_timestamp() + ($1::bigint * interval '1 millisecond'),
		    updated_at = clock_timestamp()
		FROM candidate
		WHERE jq.id = candidate.id
		RETURNING jq.id, jq.kind, jq.work_item_id, jq.state, jq.payload,
		          jq.attempts, jq.lease_until, jq.created_at, jq.updated_at
	`, leaseMillis, KindDispatch)
	job, err := scanJob(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Job{}, false, nil
		}
		return Job{}, false, fmt.Errorf("jobqueue: claim next: %w", err)
	}
	return job, true, nil
}

func scanJob(row pgx.Row) (Job, error) {
	var (
		job   Job
		state string
	)
	if err := row.Scan(
		&job.ID,
		&job.Kind,
		&job.WorkItemID,
		&state,
		&job.Payload,
		&job.Attempts,
		&job.LeaseUntil,
		&job.CreatedAt,
		&job.UpdatedAt,
	); err != nil {
		return Job{}, err
	}
	job.State = JobState(state)
	return job, nil
}

func (s *Service) MarkDone(ctx context.Context, id uuid.UUID, expectedAttempt int) error {
	return s.markTerminal(ctx, id, JobDone, expectedAttempt)
}

func (s *Service) MarkFailed(ctx context.Context, id uuid.UUID, expectedAttempt int) error {
	return s.markTerminal(ctx, id, JobFailed, expectedAttempt)
}

func (s *Service) MarkCanceled(ctx context.Context, id uuid.UUID, expectedAttempt int) error {
	return s.markTerminal(ctx, id, JobCanceled, expectedAttempt)
}

func (s *Service) markTerminal(ctx context.Context, id uuid.UUID, state JobState, expectedAttempt int) error {
	if expectedAttempt <= 0 {
		return ErrInvalidAttempt
	}
	tag, err := s.pool.Exec(ctx, `
		WITH locked AS MATERIALIZED (
			SELECT id
			FROM job_queue
			WHERE id = $1
			FOR UPDATE
		)
		UPDATE job_queue jq
		SET state = $2,
		    lease_until = NULL,
		    updated_at = clock_timestamp()
		FROM locked
		WHERE jq.id = locked.id
		  AND jq.state = 'leased'
		  AND jq.attempts = $3
		  AND jq.lease_until > clock_timestamp()
	`, id, string(state), expectedAttempt)
	if err != nil {
		return fmt.Errorf("jobqueue: mark %s: %w", state, err)
	}
	if tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}
