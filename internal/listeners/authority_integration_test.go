package listeners_test

// LCP2-B3 regression: listener administration authority is a DOMAIN
// invariant, enforced inside the service by the single access reducer — not
// a transport convention. These tests call the service DIRECTLY, the exact
// bypass route the finding described: an internal caller (reconciler, CLI,
// future adapter) must hit the same wall the transports do.

import (
	"context"
	"errors"
	"testing"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/listeners"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/testutil/pgtest"
)

func TestListenerServiceAuthorityIsDomainInvariant(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.NewPool(t, "listener_authority")
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	writer := app.NewEventWriter()
	authSvc := auth.NewService(pool, writer)
	root, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "authority-root", IsRoot: true, Source: domain.SourceHuman})
	if err != nil {
		t.Fatal(err)
	}
	mint := func(name string, source domain.Source, scopes ...string) domain.Token {
		result, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: name, Source: source, Scopes: scopes, Actor: &root.Token})
		if err != nil {
			t.Fatalf("mint %s: %v", name, err)
		}
		return result.Token
	}
	scopedAdmin := mint("authority-admin", domain.SourceHuman, access.ScopeListenersAdmin)
	unscopedHuman := mint("authority-unscoped-human", domain.SourceHuman)
	scopedAgent := mint("authority-scoped-agent", domain.SourceAgent, access.ScopeListenersAdmin)
	principal := mint("authority-principal", domain.SourceAgent)

	svc := listeners.NewService(pool, writer)
	register := func(name string, actor domain.Token) (listeners.Registration, error) {
		return svc.Register(ctx, listeners.RegisterInput{
			Name: name, PrincipalTokenID: principal.ID,
			Capabilities: []string{"review.complementary"}, Actor: actor,
		})
	}

	// Register: only the scoped, non-root human passes the reducer. The
	// legacy unscoped-token compatibility path deliberately does NOT apply,
	// root stays mint/revoke-only, and the admin scope on an agent is inert.
	for _, deny := range []struct {
		name  string
		actor domain.Token
	}{
		{"legacy unscoped human", unscopedHuman},
		{"agent holding the admin scope", scopedAgent},
		{"root", root.Token},
	} {
		if _, err := register("authority-denied-"+deny.actor.ID.String()[:8], deny.actor); !errors.Is(err, listeners.ErrNotAuthorized) {
			t.Errorf("Register by %s: err = %v, want ErrNotAuthorized", deny.name, err)
		}
	}
	reg, err := register("authority-listener", scopedAdmin)
	if err != nil {
		t.Fatalf("Register by scoped admin: %v", err)
	}

	// BindCredential and Retire enforce the same reducer directly.
	if _, err := svc.BindCredential(ctx, reg.ID, principal.ID, unscopedHuman); !errors.Is(err, listeners.ErrNotAuthorized) {
		t.Errorf("BindCredential by unscoped human: err = %v, want ErrNotAuthorized", err)
	}

	// SetPolicy carries no admin-surface flag any caller could forge: the
	// service decides from the actor token alone. The principal cannot set
	// the INITIAL policy — the baseline lens is administration's to define.
	basePolicy := listeners.Policy{
		Capabilities:             []string{"review.complementary"},
		MaxConcurrentAssignments: 1,
		Focus:                    listeners.FocusClaimedWorkItemTree,
	}
	if _, err := svc.SetPolicy(ctx, reg.ID, listeners.SetPolicyInput{Policy: basePolicy, Actor: principal}); !errors.Is(err, listeners.ErrNotAuthorized) {
		t.Errorf("initial SetPolicy by principal: err = %v, want ErrNotAuthorized", err)
	}
	if _, err := svc.SetPolicy(ctx, reg.ID, listeners.SetPolicyInput{Policy: basePolicy, Actor: unscopedHuman}); !errors.Is(err, listeners.ErrNotAuthorized) {
		t.Errorf("initial SetPolicy by unscoped human: err = %v, want ErrNotAuthorized", err)
	}
	withPolicy, err := svc.SetPolicy(ctx, reg.ID, listeners.SetPolicyInput{Policy: basePolicy, Actor: scopedAdmin})
	if err != nil {
		t.Fatalf("initial SetPolicy by scoped admin: %v", err)
	}

	// LCP2-B2 regression through the real service path: with everything else
	// identical, the principal flipping focus claimed_work_item_tree ->
	// retain_base is a widening and must refuse; no admin-surface flag exists
	// to smuggle it through.
	widerFocus := basePolicy
	widerFocus.Focus = listeners.FocusRetainBase
	if _, err := svc.SetPolicy(ctx, reg.ID, listeners.SetPolicyInput{
		Policy:                widerFocus,
		ObservedPolicyEventID: withPolicy.PolicyEventID,
		Actor:                 principal,
	}); !errors.Is(err, listeners.ErrNotAuthorized) {
		t.Errorf("principal focus widening: err = %v, want ErrNotAuthorized", err)
	}

	// The same replacement from administration is a legitimate wide move.
	if _, err := svc.SetPolicy(ctx, reg.ID, listeners.SetPolicyInput{
		Policy:                widerFocus,
		ObservedPolicyEventID: withPolicy.PolicyEventID,
		Actor:                 scopedAdmin,
	}); err != nil {
		t.Errorf("admin focus replacement: %v", err)
	}

	if _, err := svc.Retire(ctx, reg.ID, "authority fixture", scopedAgent); !errors.Is(err, listeners.ErrNotAuthorized) {
		t.Errorf("Retire by scoped agent: err = %v, want ErrNotAuthorized", err)
	}
}
