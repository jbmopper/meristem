package main

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/feed"
	"github.com/jbmopper/meristem/internal/projectiondefs"
)

var projectionSeedDefinitions = []projectiondefs.DefineInput{
	{
		Name:      "activity",
		Version:   1,
		Type:      projectiondefs.ProjectionTypeFeed,
		Rootstock: true,
		Filter: feed.ProjectionFilter{
			KindClasses: []string{
				feed.KindClassLifecycle,
				feed.KindClassDecision,
				feed.KindClassProgress,
			},
		},
		Description: "Rootstock feed projection for non-admin operator activity.",
	},
	{
		Name:      "owner-attention",
		Version:   1,
		Type:      projectiondefs.ProjectionTypeFeed,
		Rootstock: true,
		Filter: feed.ProjectionFilter{
			Kinds: []string{
				domain.EventEscalationRequested,
				domain.EventPatienceBreached,
			},
		},
		Description: "Rootstock feed projection for items that need owner attention.",
	},
	{
		Name:      "dispatch",
		Version:   1,
		Type:      projectiondefs.ProjectionTypeFeed,
		Rootstock: true,
		Filter: feed.ProjectionFilter{
			Kinds: []string{
				domain.EventDispatchRequested,
			},
		},
		Description: "Rootstock feed projection for items ready for agent dispatch.",
	},
}

func seedProjectionFixtures(ctx context.Context, pool *pgxpool.Pool, writer *events.Writer, actor domain.Token) (created, replayed int, err error) {
	svc := projectiondefs.NewService(pool, writer)
	for _, item := range projectionSeedDefinitions {
		_, fresh, defineErr := svc.Define(ctx, actor, item)
		if defineErr != nil {
			return created, replayed, fmt.Errorf("seed projection %s@%d: %w", item.Name, item.Version, defineErr)
		}
		if fresh {
			created++
		} else {
			replayed++
		}
	}
	return created, replayed, nil
}

func projectionSeedTotal() int {
	return len(projectionSeedDefinitions)
}
