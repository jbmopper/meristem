package worker

// LCP2-R2-B2 (reconciler half): dispatch.requested is the durable demand
// envelope listener resolution binds to, so the reconciler must record the
// demanded semantic capability, exact cultivar, and the ORIGINATING principal — the last non-system
// principal that advanced the item, not the item's creator and not the
// system author of the dispatch event itself.

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/workitems"
)

func TestDispatchCarriesDemandRoutingMetadata(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	writer := app.NewEventWriter()
	authSvc := auth.NewService(pool, writer)
	root, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "dispatch-origin-root", IsRoot: true, Source: domain.SourceHuman})
	if err != nil {
		t.Fatalf("create root: %v", err)
	}
	systemTok, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "worker-dispatch-origin", Source: domain.SourceSystem, Actor: &root.Token})
	if err != nil {
		t.Fatalf("create system token: %v", err)
	}
	creator, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "dispatch-origin-creator", Source: domain.SourceHuman, Actor: &root.Token})
	if err != nil {
		t.Fatalf("create creator: %v", err)
	}
	author, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "dispatch-origin-author", Source: domain.SourceAgent, Actor: &root.Token})
	if err != nil {
		t.Fatalf("create author: %v", err)
	}
	seedFastWorkerCultivar(t, ctx, pool, writer, systemTok.Token, 60)

	service := workitems.NewService(pool, writer)
	item, err := service.Create(ctx, workitems.CreateInput{
		Title:                      "dispatch origin attribution",
		State:                      domain.WorkItemTriaged,
		SuggestedConvergenceChecks: []string{"cmd:go test ./..."},
		Cultivar:                   "fast-worker@1",
		Actor:                      creator.Token,
	})
	if err != nil {
		t.Fatalf("create item: %v", err)
	}
	// The agent authors the latest substantive event: origin must attribute
	// to the AUTHOR, not the human creator.
	if err := service.AppendEvent(ctx, item.ID, "agent.status", map[string]any{"note": "implementation delivered"}, author.Token); err != nil {
		t.Fatalf("append author event: %v", err)
	}

	now := time.Now().UTC()
	w, err := New(pool, writer, Budgets{ByState: map[domain.WorkItemState]time.Duration{
		domain.WorkItemTriaged: 24 * time.Hour,
	}}, &systemTok.Token.ID, func() time.Time { return now })
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	result, err := w.ScanOnce(ctx)
	if err != nil {
		t.Fatalf("ScanOnce: %v", err)
	}
	if result.DispatchesRequested != 1 {
		t.Fatalf("dispatch requested = %d, want 1", result.DispatchesRequested)
	}

	var raw []byte
	if err := pool.QueryRow(ctx,
		`SELECT payload FROM events WHERE subject_kind=$1 AND subject_id=$2 AND kind=$3`,
		domain.SubjectWorkItem, item.ID, domain.EventDispatchRequested).Scan(&raw); err != nil {
		t.Fatalf("load dispatch payload: %v", err)
	}
	var payload struct {
		Capability    string    `json:"capability"`
		Cultivar      string    `json:"cultivar"`
		OriginTokenID uuid.UUID `json:"origin_token_id"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("decode dispatch payload: %v", err)
	}
	if payload.Cultivar != "fast-worker@1" {
		t.Fatalf("cultivar = %q, want fast-worker@1", payload.Cultivar)
	}
	if payload.Capability != "cultivar.fast-worker.v1" {
		t.Fatalf("capability = %q, want deterministic custom-cultivar fallback cultivar.fast-worker.v1", payload.Capability)
	}
	if payload.OriginTokenID != author.Token.ID {
		t.Fatalf("origin_token_id = %s, want latest non-system author %s (creator %s, system %s)",
			payload.OriginTokenID, author.Token.ID, creator.Token.ID, systemTok.Token.ID)
	}
}
