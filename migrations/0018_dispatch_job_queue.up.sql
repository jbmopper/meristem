-- 0018_dispatch_job_queue: backfill durable queue rows for existing dispatch facts.
--
-- New dispatch.requested events enqueue through the application projector.
-- This migration covers dispatch events already present before that projector
-- shipped. The job id is the dispatch event id: one durable cause, one job.

INSERT INTO job_queue (id, kind, work_item_id, state, payload, created_at, updated_at)
SELECT e.id, 'dispatch', e.subject_id, 'pending', e.payload, e.occurred_at, e.occurred_at
FROM events e
JOIN work_items wi ON wi.id = e.subject_id
WHERE e.subject_kind = 'work_item'
  AND e.kind = 'dispatch.requested'
ON CONFLICT (id) DO NOTHING;
