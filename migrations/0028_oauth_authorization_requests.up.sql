CREATE TABLE oauth_authorization_requests (
    id UUID PRIMARY KEY,
    work_item_id UUID NOT NULL UNIQUE REFERENCES work_items(id),
    approval_id UUID NOT NULL UNIQUE REFERENCES approvals(id),
    client_id TEXT NOT NULL,
    redirect_uri TEXT NOT NULL,
    response_type TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT '',
    code_challenge TEXT NOT NULL,
    code_challenge_method TEXT NOT NULL,
    scope TEXT NOT NULL,
    resource TEXT NOT NULL,
    actor_token_id UUID NOT NULL REFERENCES tokens(id),
    authority_profile TEXT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    outcome TEXT CHECK (outcome IN ('approved','denied','expired')),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX oauth_authorization_requests_pending_idx
    ON oauth_authorization_requests (expires_at) WHERE completed_at IS NULL;
