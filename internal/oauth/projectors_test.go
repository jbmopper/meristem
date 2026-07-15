package oauth

import (
	"context"
	"encoding/base64"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/testutil/pgtest"
)

func TestOAuthProjectorsFailClosedOnUnknownPayloadVersion(t *testing.T) {
	tests := []struct {
		name      string
		projector interface {
			Apply(context.Context, pgx.Tx, domain.Event) error
		}
		subject string
	}{
		{"client registered", registeredProjector{}, domain.SubjectOAuthClient},
		{"client actor bound", clientActorBoundProjector{}, domain.SubjectOAuthClient},
		{"client binding requested", clientActorBindingRequestedProjector{}, domain.SubjectOAuthClient},
		{"client revoked", clientRevokedProjector{}, domain.SubjectOAuthClient},
		{"authorization request created", authorizationRequestCreatedProjector{}, domain.SubjectOAuthAuthorizationRequest},
		{"authorization request completed", authorizationRequestCompletedProjector{}, domain.SubjectOAuthAuthorizationRequest},
		{"code issued", codeIssuedProjector{}, domain.SubjectOAuthAuthorizationCode},
		{"code redeemed", codeRedeemedProjector{}, domain.SubjectOAuthAuthorizationCode},
		{"grant issued", grantIssuedProjector{}, domain.SubjectOAuthGrant},
		{"grant refreshed", grantRefreshedProjector{}, domain.SubjectOAuthGrant},
		{"grant revoked", grantRevokedProjector{}, domain.SubjectOAuthGrant},
		{"refresh reuse", refreshReuseProjector{}, domain.SubjectOAuthGrant},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.projector.Apply(context.Background(), nil, domain.Event{
				SubjectKind: tt.subject,
				SubjectID:   uuid.New(),
				Payload:     map[string]any{"payload_version": 99},
			})
			if err == nil || !strings.Contains(err.Error(), "unknown payload_version 99") {
				t.Fatalf("expected unknown-version error, got %v", err)
			}
		})
	}
}

func TestOAuthProjectorsRejectPayloadIdentityAndMalformedFields(t *testing.T) {
	grantID := uuid.New()
	sha := base64.StdEncoding.EncodeToString(make([]byte, 32))
	tests := []struct {
		name      string
		projector interface {
			Apply(context.Context, pgx.Tx, domain.Event) error
		}
		event domain.Event
		want  string
	}{
		{
			name:      "client subject mismatch",
			projector: clientActorBoundProjector{},
			event: domain.Event{SubjectKind: domain.SubjectOAuthClient, SubjectID: uuid.New(), Payload: map[string]any{
				"payload_version": 1, "client_id": "mcpc_one", "actor_token_id": uuid.New(), "authority_profile": "owner_tracker_read_v1",
			}},
			want: "client_id",
		},
		{
			name:      "grant subject mismatch",
			projector: grantRefreshedProjector{},
			event: domain.Event{SubjectKind: domain.SubjectOAuthGrant, SubjectID: uuid.New(), Payload: map[string]any{
				"payload_version": 1, "grant_id": grantID, "old_refresh_token_id": "old", "new_refresh_token_id": "new", "new_refresh_token_hash_b64": sha, "access_token_id": "access", "access_token_hash_b64": sha, "generation": 2, "refresh_expires_at_unix": 200, "access_expires_at_unix": 100,
			}},
			want: "grant_id",
		},
		{
			name:      "short token hash",
			projector: grantIssuedProjector{},
			event: domain.Event{SubjectKind: domain.SubjectOAuthGrant, SubjectID: grantID, Payload: map[string]any{
				"payload_version": 1, "grant_id": grantID, "actor_token_id": uuid.New(), "client_id": "mcpc_one", "code_id": "codeabc", "authority_profile": "owner_tracker_read_v1", "scope": ScopeMCPRead, "resource": "https://example.test/mcp", "access_token_id": "access", "access_token_hash_b64": base64.StdEncoding.EncodeToString([]byte("short")), "refresh_token_id": "refresh", "refresh_token_hash_b64": sha, "access_expires_at_unix": 100, "refresh_expires_at_unix": 200, "generation": 1,
			}},
			want: "32-byte",
		},
		{
			name:      "invalid completion outcome",
			projector: authorizationRequestCompletedProjector{},
			event: domain.Event{SubjectKind: domain.SubjectOAuthAuthorizationRequest, SubjectID: grantID, Payload: map[string]any{
				"payload_version": 1, "authorization_request_id": grantID, "outcome": "maybe", "completed_at_unix": 100,
			}},
			want: "outcome invalid",
		},
		{
			name:      "malformed refresh reuse",
			projector: refreshReuseProjector{},
			event: domain.Event{SubjectKind: domain.SubjectOAuthGrant, SubjectID: grantID, Payload: map[string]any{
				"payload_version": 1, "grant_id": grantID, "token_id": "", "detected_at_unix": 100, "reason": "replayed",
			}},
			want: "token_id",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.projector.Apply(context.Background(), nil, tt.event)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("expected error containing %q, got %v", tt.want, err)
			}
		})
	}
}

func TestOAuthUpdateProjectorsRejectMissingDependencies(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.NewPool(t, "oauth_projector_dependencies")
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1_700_000_000, 0).UTC()
	sha := base64.StdEncoding.EncodeToString(make([]byte, 32))
	grantID := uuid.New()
	tests := []struct {
		name      string
		projector interface {
			Apply(context.Context, pgx.Tx, domain.Event) error
		}
		event domain.Event
		want  string
	}{
		{
			name:      "actor binding missing client",
			projector: clientActorBoundProjector{},
			event: domain.Event{SubjectKind: domain.SubjectOAuthClient, SubjectID: ClientSubjectID("missing"), OccurredAt: now, Payload: map[string]any{
				"payload_version": 1, "client_id": "missing", "actor_token_id": uuid.New(), "authority_profile": "owner_tracker_read_v1",
			}},
			want: "affected 0",
		},
		{
			name:      "code redemption missing code",
			projector: codeRedeemedProjector{},
			event: domain.Event{SubjectKind: domain.SubjectOAuthAuthorizationCode, SubjectID: CodeSubjectID("missing"), OccurredAt: now, Payload: redeemedPayload{
				PayloadVersion: 1, CodeID: "missing", RedeemedAtUnix: now.Unix(),
			}},
			want: "affected 0",
		},
		{
			name:      "refresh missing predecessor",
			projector: grantRefreshedProjector{},
			event: domain.Event{SubjectKind: domain.SubjectOAuthGrant, SubjectID: grantID, OccurredAt: now, Payload: map[string]any{
				"payload_version": 1, "grant_id": grantID, "old_refresh_token_id": "missing", "new_refresh_token_id": "new", "new_refresh_token_hash_b64": sha, "access_token_id": "access", "access_token_hash_b64": sha, "generation": 2, "refresh_expires_at_unix": now.Add(2 * time.Hour).Unix(), "access_expires_at_unix": now.Add(time.Hour).Unix(),
			}},
			want: "affected 0",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tx, err := pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback(ctx) }()
			err = tt.projector.Apply(ctx, tx, tt.event)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("expected dependency error containing %q, got %v", tt.want, err)
			}
		})
	}
}
