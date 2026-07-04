-- 0014_policy_profile: projection of the active safety policy profile.
--
-- Single-row table: the active profile is a singleton aggregate projected
-- from policy_profile.switched events. Absent row means "steady" (the
-- pre-profile default), so old binaries and un-switched systems behave
-- identically. Expand-safe: new table only, no existing writers affected.

CREATE TABLE active_policy_profile (
    singleton    BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    name         TEXT NOT NULL,
    fingerprint  TEXT NOT NULL,
    switched_at  TIMESTAMPTZ NOT NULL,
    switched_by  UUID
);
