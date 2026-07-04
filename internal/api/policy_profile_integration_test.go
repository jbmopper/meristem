package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/safety"
	"github.com/jbmopper/meristem/internal/storage"
)

// TestPolicyProfileSwitchIntegration covers R4's convergence checks end to
// end: /readyz reports the active profile fingerprint, a switch appends one
// attributed policy_profile.switched event and moves the projection, agent
// tokens cannot switch, and re-switching to the active profile is a no-op.
func TestPolicyProfileSwitchIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	authSvc := auth.NewService(pool, app.NewEventWriter())
	rootResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "profile-root",
		IsRoot: true,
		Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create root token: %v", err)
	}
	root := rootResult.Token
	agent, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "profile-agent",
		Source: domain.SourceAgent,
		Actor:  &root,
	})
	if err != nil {
		t.Fatalf("create agent token: %v", err)
	}

	server := New(pool, nil)

	steadyFP := profileFingerprint(t, safety.ProfileSteady)
	bringUpFP := profileFingerprint(t, safety.ProfileBringUp)

	// Un-switched system reports steady.
	ready := doReadyz(t, server.Handler())
	if ready["policy_profile"] != safety.ProfileSteady || ready["safety_policy"] != steadyFP {
		t.Fatalf("un-switched readyz: want steady/%s, got %v", steadyFP, ready)
	}

	// Agent tokens cannot switch, and the refusal appends nothing.
	before := totalEventCount(t, pool)
	denied := doREST(t, server.Handler(), http.MethodPost, "/v1/policy-profile", agent.Secret, "profile-denied", []byte(`{"profile":"bring-up"}`))
	assertRESTStatus(t, denied, http.StatusForbidden)
	assertErrorCode(t, denied, "human_token_required")
	if after := totalEventCount(t, pool); after != before {
		t.Fatalf("denied switch appended events: before=%d after=%d", before, after)
	}

	// Human switch to bring-up: attributed event + projection + readyz.
	switched := doREST(t, server.Handler(), http.MethodPost, "/v1/policy-profile", rootResult.Secret, "profile-switch-1", []byte(`{"profile":"bring-up"}`))
	assertRESTStatus(t, switched, http.StatusOK)
	var resp policyProfileResponse
	if err := json.Unmarshal(switched.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode switch response: %v", err)
	}
	if resp.Profile != safety.ProfileBringUp || resp.Fingerprint != bringUpFP || !resp.Switched {
		t.Fatalf("switch response: %+v (want bring-up/%s/switched)", resp, bringUpFP)
	}
	assertSwitchEventCount(t, pool, 1)
	assertSwitchEventAttributed(t, pool, root.ID)
	ready = doReadyz(t, server.Handler())
	if ready["policy_profile"] != safety.ProfileBringUp || ready["safety_policy"] != bringUpFP {
		t.Fatalf("post-switch readyz: want bring-up/%s, got %v", bringUpFP, ready)
	}

	// Re-switch to the active profile: no-op, no new event.
	again := doREST(t, server.Handler(), http.MethodPost, "/v1/policy-profile", rootResult.Secret, "profile-switch-2", []byte(`{"profile":"bring-up"}`))
	assertRESTStatus(t, again, http.StatusOK)
	var againResp policyProfileResponse
	if err := json.Unmarshal(again.Body.Bytes(), &againResp); err != nil {
		t.Fatalf("decode re-switch response: %v", err)
	}
	if againResp.Switched {
		t.Fatalf("re-switch to active profile must be a no-op, got %+v", againResp)
	}
	assertSwitchEventCount(t, pool, 1)

	// Unknown profile is a structured refusal.
	unknown := doREST(t, server.Handler(), http.MethodPost, "/v1/policy-profile", rootResult.Secret, "profile-switch-3", []byte(`{"profile":"mellow"}`))
	assertRESTStatus(t, unknown, http.StatusUnprocessableEntity)
	assertErrorCode(t, unknown, "invalid_policy_profile")

	// Switch back: distinct action, second event, steady restored.
	back := doREST(t, server.Handler(), http.MethodPost, "/v1/policy-profile", rootResult.Secret, "profile-switch-4", []byte(`{"profile":"steady"}`))
	assertRESTStatus(t, back, http.StatusOK)
	assertSwitchEventCount(t, pool, 2)
	ready = doReadyz(t, server.Handler())
	if ready["policy_profile"] != safety.ProfileSteady || ready["safety_policy"] != steadyFP {
		t.Fatalf("switched-back readyz: want steady/%s, got %v", steadyFP, ready)
	}
}

func profileFingerprint(t *testing.T, name string) string {
	t.Helper()
	p, err := safety.ProfileByName(name)
	if err != nil {
		t.Fatalf("profile %q: %v", name, err)
	}
	fp, err := p.Fingerprint()
	if err != nil {
		t.Fatalf("fingerprint %q: %v", name, err)
	}
	return fp
}

func doReadyz(t *testing.T, handler http.Handler) map[string]string {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("readyz: want 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	var out map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode readyz: %v", err)
	}
	return out
}

func assertSwitchEventCount(t *testing.T, pool *pgxpool.Pool, want int) {
	t.Helper()
	var got int
	if err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM events WHERE kind = $1`, domain.EventPolicyProfileSwitched).Scan(&got); err != nil {
		t.Fatalf("count switch events: %v", err)
	}
	if got != want {
		t.Fatalf("policy_profile.switched events: want %d, got %d", want, got)
	}
}

func assertSwitchEventAttributed(t *testing.T, pool *pgxpool.Pool, actor interface{ String() string }) {
	t.Helper()
	var count int
	if err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM events WHERE kind = $1 AND actor_token_id = $2 AND source = 'human'`,
		domain.EventPolicyProfileSwitched, actor.String()).Scan(&count); err != nil {
		t.Fatalf("check switch attribution: %v", err)
	}
	if count == 0 {
		t.Fatal("switch event is not attributed to the switching human token")
	}
}
