package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/registry"
)

var registrySeedTropisms = []registry.DefineTropismInput{
	{
		Name:    "checklist-all",
		Version: 1,
		Reducer: registry.ReducerRef{
			Identity: "all_pass_checklist",
			Version:  1,
		},
		Params:      mustJSON(`{"budget":{"max_attempts":3,"escalation":"hand_to_human"}}`),
		Description: "All declared checklist items must pass; missing or failing checks reject.",
	},
	{
		Name:    "checks-proposal",
		Version: 1,
		Reducer: registry.ReducerRef{
			Identity: "checks_proposal",
			Version:  1,
		},
		Params:      mustJSON(`{"budget":{"max_attempts":3,"escalation":"hand_to_human"}}`),
		Description: "Validate a scribe-authored convergence-check proposal before it lands on the parent.",
	},
	{
		Name:    "human-ack",
		Version: 1,
		Reducer: registry.ReducerRef{
			Identity: "human_ack",
			Version:  1,
		},
		Params:      mustJSON(`{"budget":{"max_attempts":1,"escalation":"hand_to_human"}}`),
		Description: "Follow an explicit owner acknowledgement signal.",
	},
}

var registrySeedCultivars = []registry.DefineCultivarInput{
	{
		Name:      "convergence-scribe",
		Version:   1,
		Rootstock: true,
		Tropism:   registry.TropismRef{Name: "checklist-all", Version: 1},
		Profile: registry.Profile{
			Briefing: "briefings/convergence-scribe.md",
			ScopesTemplate: []string{
				"work_items.tree:{root}",
				"work_items.read",
				"work_items.write",
				"feed.read_assigned",
			},
		},
		Xylem: registry.Xylem{
			MaxAttempts:    3,
			MaxWallSeconds: 1800,
			MaxDepth:       1,
		},
		Phloem:      "projection:work-item-brief",
		Description: "Rootstock worker that proposes convergence checks for a checkless parent item.",
	},
	{
		Name:      "human-attention",
		Version:   1,
		Rootstock: true,
		Tropism:   registry.TropismRef{Name: "human-ack", Version: 1},
		Profile: registry.Profile{
			Briefing: "briefings/human-attention.md",
			ScopesTemplate: []string{
				"work_items.tree:{root}",
				"work_items.read",
				"work_items.write",
				"feed.read_assigned",
			},
		},
		Xylem: registry.Xylem{
			MaxAttempts:    1,
			MaxWallSeconds: 604800,
			MaxDepth:       0,
		},
		Phloem:      "projection:human-attention-brief",
		Description: "Rootstock escalation shape for direct owner attention.",
	},
	{
		Name:      "checklist-worker",
		Version:   1,
		Rootstock: true,
		Tropism:   registry.TropismRef{Name: "checklist-all", Version: 1},
		Profile: registry.Profile{
			Briefing: "briefings/checklist-worker.md",
			ScopesTemplate: []string{
				"work_items.tree:{root}",
				"work_items.read",
				"work_items.write",
				"feed.read_assigned",
			},
		},
		Xylem: registry.Xylem{
			MaxAttempts:    3,
			MaxWallSeconds: 3600,
			MaxDepth:       1,
		},
		Phloem:      "projection:work-item-brief",
		Description: "Rootstock worker for ordinary declared-check execution.",
	},
}

func seedRegistryFixtures(ctx context.Context, pool *pgxpool.Pool, writer *events.Writer, actor domain.Token) (created, replayed int, err error) {
	svc := registry.NewService(pool, writer)
	for _, item := range registrySeedTropisms {
		_, fresh, defineErr := svc.DefineTropism(ctx, actor, item)
		if defineErr != nil {
			return created, replayed, fmt.Errorf("seed registry tropism %s@%d: %w", item.Name, item.Version, defineErr)
		}
		if fresh {
			created++
		} else {
			replayed++
		}
	}
	for _, item := range registrySeedCultivars {
		_, fresh, defineErr := svc.DefineCultivar(ctx, actor, item)
		if defineErr != nil {
			return created, replayed, fmt.Errorf("seed registry cultivar %s@%d: %w", item.Name, item.Version, defineErr)
		}
		if fresh {
			created++
		} else {
			replayed++
		}
	}
	return created, replayed, nil
}

func registrySeedTotal() int {
	return len(registrySeedTropisms) + len(registrySeedCultivars)
}

func mustJSON(raw string) json.RawMessage {
	if !json.Valid([]byte(raw)) {
		panic("invalid registry seed JSON: " + raw)
	}
	return json.RawMessage(raw)
}
