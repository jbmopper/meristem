-- Listener-bound claims (listener control plane, slice 3): an assignment
-- claimed through a listener records WHICH listener, under WHICH policy
-- revision, for WHICH durable demand event. Restart derivation and
-- completion are generation-bound to the listener, not just the token, so a
-- principal backing multiple listener registrations can prove which
-- listener/policy generation owns each lease. NULL columns are ordinary
-- (non-listener) claims.
ALTER TABLE work_item_assignment_state
    ADD COLUMN listener_id uuid REFERENCES listener_registrations(id),
    ADD COLUMN demand_event_id uuid,
    ADD COLUMN policy_event_id uuid;

CREATE INDEX work_item_assignment_state_listener_idx
    ON work_item_assignment_state (listener_id)
    WHERE listener_id IS NOT NULL;
