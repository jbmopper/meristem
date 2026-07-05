package main

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/registry"
	"github.com/jbmopper/meristem/internal/storage"
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
