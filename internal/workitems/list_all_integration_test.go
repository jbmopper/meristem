package workitems

import (
	"context"
	"fmt"
	"testing"

	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/projections"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/testutil/pgtest"
)

func TestListAllIncludesOlderItemBeyondBoundedListWindow(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.NewPool(t, "meristem_list_all")
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	registry := projections.NewRegistry()
	auth.RegisterProjectors(registry)
	RegisterProjectors(registry)
	writer := events.NewWriter(registry)
	rootResult, err := auth.NewService(pool, writer).CreateToken(ctx, auth.CreateTokenInput{
		Name: "list-all-root", IsRoot: true, Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create root token: %v", err)
	}
	service := NewService(pool, writer)
	older, err := service.Create(ctx, CreateInput{Title: "older open item", Actor: rootResult.Token})
	if err != nil {
		t.Fatalf("create older item: %v", err)
	}
	for i := 0; i < 201; i++ {
		if _, err := service.Create(ctx, CreateInput{
			Title: fmt.Sprintf("newer item %03d", i),
			Actor: rootResult.Token,
		}); err != nil {
			t.Fatalf("create newer item %d: %v", i, err)
		}
	}

	items, err := service.ListAll(ctx, "")
	if err != nil {
		t.Fatalf("ListAll: %v", err)
	}
	if got, want := len(items), 202; got != want {
		t.Fatalf("ListAll returned %d items, want %d", got, want)
	}
	found := false
	for _, item := range items {
		if item.ID == older.ID {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("ListAll omitted older item %s after 201 newer rows", older.ID)
	}

	bounded, err := service.List(ctx, "", 200)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if got, want := len(bounded), 200; got != want {
		t.Fatalf("bounded List returned %d items, want %d", got, want)
	}
}
