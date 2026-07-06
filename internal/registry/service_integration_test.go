package registry

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/projections"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/testutil/pgtest"
)

func TestRegistryServiceIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	projectionRegistry := projections.NewRegistry()
	auth.RegisterProjectors(projectionRegistry)
	RegisterProjectors(projectionRegistry)
	writer := events.NewWriter(projectionRegistry)
	tokenResult, err := auth.NewService(pool, writer).CreateToken(ctx, auth.CreateTokenInput{
		Name:   "registry-integration",
		IsRoot: true,
		Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create token: %v", err)
	}
	actor := tokenResult.Token
	svc := NewService(pool, writer)

	tropism := DefineTropismInput{
		Name:        "checklist-all",
		Version:     1,
		Reducer:     ReducerRef{Identity: "all_pass_checklist", Version: 1},
		Params:      mustRaw(`{"budget":{"max_attempts":3,"escalation":"hand_to_human"}}`),
		Description: "all checklist items pass",
	}
	first, fresh, err := svc.DefineTropism(ctx, actor, tropism)
	if err != nil || !fresh {
		t.Fatalf("define tropism first fresh=%t err=%v", fresh, err)
	}
	if first.Version != 1 {
		t.Fatalf("first version = %d", first.Version)
	}
	_, fresh, err = svc.DefineTropism(ctx, actor, tropism)
	if err != nil || fresh {
		t.Fatalf("define tropism replay fresh=%t err=%v", fresh, err)
	}
	tropism.Version = 2
	tropism.Description = "all checklist items pass v2"
	current, fresh, err := svc.DefineTropism(ctx, actor, tropism)
	if err != nil || !fresh || current.Version != 2 {
		t.Fatalf("define tropism v2 current=%+v fresh=%t err=%v", current, fresh, err)
	}
	tropism.Version = 1
	tropism.Description = "all checklist items pass"
	current, fresh, err = svc.DefineTropism(ctx, actor, tropism)
	if err != nil || fresh || current.Version != 2 {
		t.Fatalf("historical tropism replay current=%+v fresh=%t err=%v", current, fresh, err)
	}
	bad := tropism
	bad.Name = "judge"
	bad.Version = 1
	bad.Reducer = ReducerRef{Identity: "judge_vote", Version: 1}
	if _, _, err := svc.DefineTropism(ctx, actor, bad); !errors.Is(err, ErrUnknownReducer) {
		t.Fatalf("expected unknown reducer, got %v", err)
	}

	cultivar := DefineCultivarInput{
		Name:      "convergence-scribe",
		Version:   1,
		Rootstock: true,
		Tropism:   TropismRef{Name: "checklist-all", Version: 1},
		Profile: Profile{
			Briefing:       "briefings/convergence-scribe.md",
			ScopesTemplate: []string{"work_items.read", "work_items.write"},
		},
		Xylem:       Xylem{MaxAttempts: 3, MaxWallSeconds: 1800, MaxDepth: 1},
		Phloem:      "projection:work-item-brief",
		Description: "scribe rootstock",
	}
	defined, fresh, err := svc.DefineCultivar(ctx, actor, cultivar)
	if err != nil || !fresh {
		t.Fatalf("define cultivar first fresh=%t err=%v", fresh, err)
	}
	if defined.Tropism.Version != 1 {
		t.Fatalf("cultivar historical tropism ref version = %d", defined.Tropism.Version)
	}
	_, fresh, err = svc.DefineCultivar(ctx, actor, cultivar)
	if err != nil || fresh {
		t.Fatalf("define cultivar replay fresh=%t err=%v", fresh, err)
	}
	cultivar.Version = 2
	if _, _, err := svc.DefineCultivar(ctx, actor, cultivar); !errors.Is(err, ErrRootstockImmutable) {
		t.Fatalf("expected rootstock immutable, got %v", err)
	}
	cultivar.Name = "unknown-tropism-worker"
	cultivar.Version = 1
	cultivar.Rootstock = false
	cultivar.Tropism = TropismRef{Name: "missing", Version: 1}
	if _, _, err := svc.DefineCultivar(ctx, actor, cultivar); !errors.Is(err, ErrUnknownTropism) {
		t.Fatalf("expected unknown tropism, got %v", err)
	}
	if _, err := svc.GetCultivar(ctx, "missing-worker"); !errors.Is(err, ErrUnknownCultivar) || !strings.Contains(err.Error(), "registry.list") {
		t.Fatalf("expected unknown cultivar naming registry.list, got %v", err)
	}
	if _, err := svc.GetTropism(ctx, "missing-tropism"); !errors.Is(err, ErrUnknownTropism) || !strings.Contains(err.Error(), "registry.list") {
		t.Fatalf("expected unknown tropism naming registry.list, got %v", err)
	}
}

func newIntegrationPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	return pgtest.NewPool(t, "meristem_registry_itest")
}

func mustRaw(raw string) []byte {
	if !json.Valid([]byte(raw)) {
		panic(raw)
	}
	return []byte(raw)
}
