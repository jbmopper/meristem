package main

// Replay honesty for the listener_registrations projection (listener control
// plane, slice 2): folding the full listener lifecycle — register, policy
// set, self-narrowing replacement, credential rotation, retirement — through
// a rebuild must reproduce the live projection byte-for-byte.

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/listeners"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/testutil/pgtest"
)

func TestListenerLifecycleRebuildHonesty(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.NewPool(t, "listener_rebuild")
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	writer := app.NewEventWriter()
	authSvc := auth.NewService(pool, writer)
	root, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "listener-rebuild-root", IsRoot: true, Source: domain.SourceHuman})
	if err != nil {
		t.Fatal(err)
	}
	admin, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "listener-rebuild-admin", Source: domain.SourceHuman, Actor: &root.Token})
	if err != nil {
		t.Fatal(err)
	}
	principal, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "listener-rebuild-principal", Source: domain.SourceAgent, Actor: &root.Token})
	if err != nil {
		t.Fatal(err)
	}
	rotated, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "listener-rebuild-rotated", Source: domain.SourceAgent, Actor: &root.Token})
	if err != nil {
		t.Fatal(err)
	}

	svc := listeners.NewService(pool, writer)
	reg, err := svc.Register(ctx, listeners.RegisterInput{
		Name: "rebuild-listener", PrincipalTokenID: principal.Token.ID,
		Provider: "codex", Capabilities: []string{"review.complementary", "implement.go"},
		Actor: admin.Token,
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	withPolicy, err := svc.SetPolicy(ctx, reg.ID, listeners.SetPolicyInput{
		Policy: listeners.Policy{
			Predicates:               []listeners.PredicateWire{{Kind: "kind_include", EventKinds: []string{"dispatch.requested"}}},
			Capabilities:             []string{"review.complementary"},
			MaxConcurrentAssignments: 1,
		},
		Actor: admin.Token, ActorIsAdminSurface: true,
	})
	if err != nil {
		t.Fatalf("set policy: %v", err)
	}
	if _, err := svc.SetPolicy(ctx, reg.ID, listeners.SetPolicyInput{
		Policy: listeners.Policy{
			Predicates: []listeners.PredicateWire{
				{Kind: "kind_include", EventKinds: []string{"dispatch.requested"}},
				{Kind: "actor", TokenIDs: []string{principal.Token.ID.String()}},
			},
			Capabilities:             []string{"review.complementary"},
			MaxConcurrentAssignments: 1,
		},
		ObservedPolicyEventID: withPolicy.PolicyEventID,
		Actor:                 principal.Token,
	}); err != nil {
		t.Fatalf("self-narrow policy: %v", err)
	}
	if _, err := svc.BindCredential(ctx, reg.ID, rotated.Token.ID, admin.Token); err != nil {
		t.Fatalf("bind credential: %v", err)
	}
	if _, err := svc.Retire(ctx, reg.ID, "rebuild fixture", admin.Token); err != nil {
		t.Fatalf("retire: %v", err)
	}

	report, err := rebuildAndDiff(ctx, pool, app.NewProjectionRegistry(), "listener_rebuild_check", slog.New(slog.NewTextHandler(io.Discard, nil)), false)
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if len(report.mismatches) != 0 {
		t.Fatalf("listener rebuild mismatches: %+v", report.mismatches)
	}
}
