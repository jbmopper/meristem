ALTER TABLE command_queue
    DROP CONSTRAINT IF EXISTS command_queue_origin_actor_source_check,
    DROP COLUMN IF EXISTS causing_work_item_id,
    DROP COLUMN IF EXISTS origin_actor_source,
    DROP COLUMN IF EXISTS origin_node_id;
