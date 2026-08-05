package listeners_test

// LCP2-R2-B1 regression, direct-service path: repeatable listener state
// changes must never collapse onto an earlier event. Event identity is
// subject + kind + canonical payload (+ discriminator); a policy A -> B -> A
// cycle and a credential X -> Y -> X -> Y cycle both repeat an earlier
// payload exactly, so without the predecessor-fallback discriminator the
// repeated event would silently reuse the first event id, the projector
// would not run, and state would remain B (codex reproduced exactly this
// with a mutation test). These calls carry NO idempotency context on
// purpose — they pin the fallback discriminator.

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/listeners"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/testutil/pgtest"
)

func countListenerEvents(t *testing.T, ctx context.Context, pool *pgxpool.Pool, listenerID uuid.UUID, kind string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM events WHERE subject_kind=$1 AND subject_id=$2 AND kind=$3`,
		domain.SubjectListener, listenerID, kind).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", kind, err)
	}
	return n
}

func TestListenerStateCyclesDoNotCollapse(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.NewPool(t, "listener_cycle")
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	writer := app.NewEventWriter()
	authSvc := auth.NewService(pool, writer)
	root, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "cycle-root", IsRoot: true, Source: domain.SourceHuman})
	if err != nil {
		t.Fatal(err)
	}
	admin, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "cycle-admin", Source: domain.SourceHuman, Scopes: []string{access.ScopeListenersAdmin}, Actor: &root.Token})
	if err != nil {
		t.Fatal(err)
	}
	principalX, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "cycle-principal-x", Source: domain.SourceAgent, Actor: &root.Token})
	if err != nil {
		t.Fatal(err)
	}
	principalY, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "cycle-principal-y", Source: domain.SourceAgent, Actor: &root.Token})
	if err != nil {
		t.Fatal(err)
	}

	svc := listeners.NewService(pool, writer)
	reg, err := svc.Register(ctx, listeners.RegisterInput{
		Name: "cycle-listener", PrincipalTokenID: principalX.Token.ID,
		Capabilities: []string{"review.complementary", "implement.go"}, Actor: admin.Token,
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	// Policy A -> B -> A: three distinct revisions, and the projection ends
	// on A, not stuck on B.
	policyA := listeners.Policy{Capabilities: []string{"review.complementary"}, MaxConcurrentAssignments: 1}
	policyB := listeners.Policy{Capabilities: []string{"implement.go"}, MaxConcurrentAssignments: 1}
	revA1, err := svc.SetPolicy(ctx, reg.ID, listeners.SetPolicyInput{Policy: policyA, Actor: admin.Token})
	if err != nil {
		t.Fatalf("set A1: %v", err)
	}
	revB, err := svc.SetPolicy(ctx, reg.ID, listeners.SetPolicyInput{Policy: policyB, ObservedPolicyEventID: revA1.PolicyEventID, Actor: admin.Token})
	if err != nil {
		t.Fatalf("set B: %v", err)
	}
	revA2, err := svc.SetPolicy(ctx, reg.ID, listeners.SetPolicyInput{Policy: policyA, ObservedPolicyEventID: revB.PolicyEventID, Actor: admin.Token})
	if err != nil {
		t.Fatalf("set A2: %v", err)
	}
	if revA2.PolicyEventID == nil || revA1.PolicyEventID == nil || *revA2.PolicyEventID == *revA1.PolicyEventID {
		t.Fatalf("repeated policy payload collapsed onto the first event: A1=%v A2=%v", revA1.PolicyEventID, revA2.PolicyEventID)
	}
	if got := countListenerEvents(t, ctx, pool, reg.ID, domain.EventListenerPolicySet); got != 3 {
		t.Fatalf("policy_set events = %d, want 3", got)
	}
	final, err := svc.Get(ctx, reg.ID)
	if err != nil {
		t.Fatalf("read final: %v", err)
	}
	if final.Policy == nil || len(final.Policy.Capabilities) != 1 || final.Policy.Capabilities[0] != "review.complementary" {
		t.Fatalf("projected policy after A->B->A is not A: %+v", final.Policy)
	}

	// Credential X -> Y -> X -> Y: the second bind-to-Y repeats the first
	// bind-to-Y payload exactly (principal Y, previous X); the predecessor
	// discriminator keeps it a distinct event and the projection lands on Y.
	if _, err := svc.BindCredential(ctx, reg.ID, principalY.Token.ID, admin.Token); err != nil {
		t.Fatalf("bind Y1: %v", err)
	}
	if _, err := svc.BindCredential(ctx, reg.ID, principalX.Token.ID, admin.Token); err != nil {
		t.Fatalf("bind X: %v", err)
	}
	rebound, err := svc.BindCredential(ctx, reg.ID, principalY.Token.ID, admin.Token)
	if err != nil {
		t.Fatalf("bind Y2: %v", err)
	}
	if rebound.PrincipalTokenID != principalY.Token.ID {
		t.Fatalf("projected principal after X->Y->X->Y = %s, want Y %s", rebound.PrincipalTokenID, principalY.Token.ID)
	}
	if got := countListenerEvents(t, ctx, pool, reg.ID, domain.EventListenerCredentialBound); got != 3 {
		t.Fatalf("credential_bound events = %d, want 3", got)
	}

	// Rebinding the already-bound principal is an idempotent no-op: no event.
	if _, err := svc.BindCredential(ctx, reg.ID, principalY.Token.ID, admin.Token); err != nil {
		t.Fatalf("no-op rebind: %v", err)
	}
	if got := countListenerEvents(t, ctx, pool, reg.ID, domain.EventListenerCredentialBound); got != 3 {
		t.Fatalf("no-op rebind appended an event: credential_bound = %d, want 3", got)
	}
}
