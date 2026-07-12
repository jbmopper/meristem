-- 0032_network_provenance: retain authenticated origin provenance and the
-- work_item whose delivery patience owns a queued command.

ALTER TABLE command_queue
    ADD COLUMN origin_node_id TEXT,
    ADD COLUMN origin_actor_source TEXT,
    ADD COLUMN causing_work_item_id UUID;

UPDATE command_queue AS cq
SET origin_node_id = COALESCE(NULLIF(e.payload->>'origin_node_id', ''), 'legacy-unknown'),
    origin_actor_source = COALESCE(NULLIF(e.payload->>'origin_actor_source', ''), e.source)
FROM events AS e
WHERE e.id = cq.id;

UPDATE command_queue
SET origin_node_id = 'legacy-unknown'
WHERE origin_node_id IS NULL;

UPDATE command_queue
SET origin_actor_source = 'system'
WHERE origin_actor_source IS NULL;

ALTER TABLE command_queue
    ALTER COLUMN origin_node_id SET NOT NULL,
    ALTER COLUMN origin_actor_source SET NOT NULL,
    ADD CONSTRAINT command_queue_origin_actor_source_check
        CHECK (origin_actor_source IN ('human', 'agent', 'system'));
