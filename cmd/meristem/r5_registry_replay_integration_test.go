package main

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/cultivaractivation"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/grants"
	"github.com/jbmopper/meristem/internal/registry"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/workitems"
)

func TestR5CultivarUpdateRebuildsFromEvents(t *testing.T) {
	ctx := context.Background()
	pool := newCmdIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	rootResult, err := auth.NewService(pool, writer).CreateToken(ctx, auth.CreateTokenInput{
		Name:   "r5-registry-root",
		IsRoot: true,
		Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create root token: %v", err)
	}

	registrySvc := registry.NewService(pool, writer)
	_, fresh, err := registrySvc.DefineTropism(ctx, rootResult.Token, registry.DefineTropismInput{
		Name:    "r5-replay-checklist",
		Version: 1,
		Reducer: registry.ReducerRef{
			Identity: "all_pass_checklist",
			Version:  1,
		},
		Params:      []byte(`{"budget":{"max_attempts":2,"escalation":"hand_to_human"}}`),
		Description: "R5 replay test tropism",
	})
	if err != nil || !fresh {
		t.Fatalf("define tropism fresh=%t err=%v", fresh, err)
	}

	cultivar := registry.DefineCultivarInput{
		Name:      "r5-replay-worker",
		Version:   1,
		Rootstock: false,
		Tropism:   registry.TropismRef{Name: "r5-replay-checklist", Version: 1},
		Profile: registry.Profile{
			Briefing: "briefings/r5-replay-worker.md",
			ScopesTemplate: []string{
				"work_items.tree:{root}",
				"work_items.read",
				"feed.read_assigned",
			},
		},
		Xylem: registry.Xylem{
			MaxAttempts:                    2,
			MaxWallSeconds:                 900,
			MaxDepth:                       1,
			MaxChildrenPerItem:             4,
			MaxEventsPerItemPerHourByClass: map[string]int{"progress": 12},
		},
		Phloem:      "projection:r5-brief",
		Description: "R5 replay test worker v1",
	}
	if _, fresh, err := registrySvc.DefineCultivar(ctx, rootResult.Token, cultivar); err != nil || !fresh {
		t.Fatalf("define cultivar v1 fresh=%t err=%v", fresh, err)
	}

	cultivar.Version = 2
	cultivar.Description = "R5 replay test worker v2"
	cultivar.Profile.ScopesTemplate = append(cultivar.Profile.ScopesTemplate, "work_items.write")
	cultivar.Xylem.MaxWallSeconds = 1200
	cultivar.Xylem.MaxConcurrentRunningPerToken = 1
	current, fresh, err := registrySvc.DefineCultivar(ctx, rootResult.Token, cultivar)
	if err != nil || !fresh {
		t.Fatalf("define cultivar v2 fresh=%t err=%v", fresh, err)
	}
	if current.Version != 2 || current.Rootstock {
		t.Fatalf("current cultivar after update = %+v", current)
	}
	if current.Description != cultivar.Description || current.Xylem.MaxWallSeconds != 1200 || current.Xylem.MaxConcurrentRunningPerToken != 1 {
		t.Fatalf("current cultivar did not project v2 payload: %+v", current)
	}

	var definitionEvents int
	if err := pool.QueryRow(ctx, `
		SELECT count(*)
		FROM events
		WHERE kind = 'cultivar.defined'
		  AND subject_kind = 'cultivar'
		  AND payload->>'name' = 'r5-replay-worker'
	`).Scan(&definitionEvents); err != nil {
		t.Fatalf("count cultivar definition events: %v", err)
	}
	if definitionEvents != 2 {
		t.Fatalf("cultivar definition events = %d, want 2", definitionEvents)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	report, err := rebuildAndDiff(ctx, pool, app.NewProjectionRegistry(), "r5_rebuild", logger, false)
	if err != nil {
		t.Fatalf("rebuild registry projections: %v", err)
	}
	if len(report.mismatches) != 0 {
		t.Fatalf("rebuild had mismatches: %+v", report.mismatches)
	}
}

func TestR5CultivarActivationRebuildsFromEvents(t *testing.T) {
	ctx := context.Background()
	pool := newCmdIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	authSvc := auth.NewService(pool, writer)
	rootResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "r5-activation-root",
		IsRoot: true,
		Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create root token: %v", err)
	}
	root := rootResult.Token
	registrySvc := registry.NewService(pool, writer)
	_, fresh, err := registrySvc.DefineTropism(ctx, root, registry.DefineTropismInput{
		Name:    "r5-activation-checklist",
		Version: 1,
		Reducer: registry.ReducerRef{
			Identity: "all_pass_checklist",
			Version:  1,
		},
		Params:      []byte(`{"budget":{"max_attempts":2,"escalation":"hand_to_human"}}`),
		Description: "R5 activation replay test tropism",
	})
	if err != nil || !fresh {
		t.Fatalf("define activation tropism fresh=%t err=%v", fresh, err)
	}

	proposal, err := workitems.NewService(pool, writer).Create(ctx, workitems.CreateInput{
		Title:             "R5 activation replay proposal",
		Actor:             root,
		HumanReviewStatus: domain.HumanReviewApproved,
	})
	if err != nil {
		t.Fatalf("create approved proposal: %v", err)
	}
	agent, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "r5-activation-agent",
		Source: domain.SourceAgent,
		Scopes: []string{
			access.ScopeWorkItemsRead,
			access.ScopeWorkItemsWrite,
			access.ScopeFeedReadAssigned,
			"work_items.tree:" + proposal.ID.String(),
		},
		Actor: &root,
	})
	if err != nil {
		t.Fatalf("create activation agent: %v", err)
	}
	beforeTokens := countCmdEvents(t, ctx, pool, domain.EventTokenCreated)
	result, err := cultivaractivation.NewService(pool, writer).Activate(ctx, cultivaractivation.ActivateInput{
		Actor:      agent.Token,
		WorkItemID: proposal.ID,
		Cultivar: registry.DefineCultivarInput{
			Name:      "r5-activated-worker",
			Version:   1,
			Rootstock: false,
			Tropism:   registry.TropismRef{Name: "r5-activation-checklist", Version: 1},
			Profile: registry.Profile{
				Briefing: "briefings/r5-activated-worker.md",
				ScopesTemplate: []string{
					"work_items.tree:{root}",
					"work_items.read",
					"work_items.write",
					"feed.read_assigned",
				},
			},
			Xylem:       registry.Xylem{MaxAttempts: 2, MaxWallSeconds: 1200, MaxDepth: 1},
			Phloem:      "projection:r5-activation-brief",
			Description: "R5 activation replay test worker",
		},
	})
	if err != nil {
		t.Fatalf("activate cultivar: %v", err)
	}
	if result.Disposition != grants.DispositionGrant || result.Cultivar == nil {
		t.Fatalf("activation result = %+v, want granted cultivar", result)
	}
	if afterTokens := countCmdEvents(t, ctx, pool, domain.EventTokenCreated); afterTokens != beforeTokens {
		t.Fatalf("activation minted token: before=%d after=%d", beforeTokens, afterTokens)
	}
	if got := countCmdEvents(t, ctx, pool, domain.EventCultivarActivationGranted); got != 1 {
		t.Fatalf("cultivar_activation.granted events = %d, want 1", got)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	report, err := rebuildAndDiff(ctx, pool, app.NewProjectionRegistry(), "r5_activation_rebuild", logger, false)
	if err != nil {
		t.Fatalf("rebuild after activation: %v", err)
	}
	if len(report.mismatches) != 0 {
		t.Fatalf("activation rebuild had mismatches: %+v", report.mismatches)
	}
}

func countCmdEvents(t *testing.T, ctx context.Context, pool *pgxpool.Pool, kind string) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM events WHERE kind = $1`, kind).Scan(&count); err != nil {
		t.Fatalf("count events for %s: %v", kind, err)
	}
	return count
}
