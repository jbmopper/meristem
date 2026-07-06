-- 0018_dispatch_job_queue rollback: remove dispatch-derived queue rows.

DELETE FROM job_queue jq
USING events e
WHERE jq.id = e.id
  AND e.subject_kind = 'work_item'
  AND e.kind = 'dispatch.requested';
