-- 0040_listener_activation_busy_dispatches: keep pre-admission target
-- backpressure out of the ordinary adapter-failure retry budget. The count is
-- a deterministic projection of adapter_target_busy failure events.

ALTER TABLE listener_activations
    ADD COLUMN busy_dispatch_count INTEGER NOT NULL DEFAULT 0
    CHECK (busy_dispatch_count >= 0 AND busy_dispatch_count <= dispatch_count);
