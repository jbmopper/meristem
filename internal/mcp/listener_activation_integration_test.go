package mcp

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/feed"
	"github.com/jbmopper/meristem/internal/idempotency"
	"github.com/jbmopper/meristem/internal/listeneractivation"
	"github.com/jbmopper/meristem/internal/listeners"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/workitems"
)

func TestListenerActivationMCPParityIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newMCPIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatal(err)
	}
	writer := app.NewEventWriter()
	authSvc := auth.NewService(pool, writer)
	root, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "activation-mcp-root", IsRoot: true, Source: domain.SourceHuman})
	if err != nil {
		t.Fatal(err)
	}
	admin, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name: "activation-mcp-admin", Source: domain.SourceHuman,
		Scopes: []string{access.ScopeListenersAdmin}, Actor: &root.Token,
	})
	if err != nil {
		t.Fatal(err)
	}
	principal, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name: "activation-mcp-principal", Source: domain.SourceAgent,
		Scopes: []string{access.ScopeWorkItemsRead, access.ScopeWorkItemsWriteAll}, Actor: &root.Token,
	})
	if err != nil {
		t.Fatal(err)
	}
	listenerSvc := listeners.NewService(pool, writer)
	reg, err := listenerSvc.Register(ctx, listeners.RegisterInput{
		Name: "activation-mcp", PrincipalTokenID: principal.Token.ID,
		Provider: "codex", Capabilities: []string{"review.complementary"}, Actor: admin.Token,
	})
	if err != nil {
		t.Fatal(err)
	}
	reg, err = listenerSvc.SetPolicy(ctx, reg.ID, listeners.SetPolicyInput{
		Policy: listeners.Policy{Capabilities: []string{"review.complementary"}, MaxConcurrentAssignments: 1}, Actor: admin.Token,
	})
	if err != nil {
		t.Fatal(err)
	}
	workSvc := workitems.NewService(pool, writer)
	item, err := workSvc.Create(ctx, workitems.CreateInput{
		Title: "activation MCP demand", State: domain.WorkItemPlanned,
		SuggestedConvergenceChecks: []string{"event:activation.completed"},
		HumanReviewStatus:          domain.HumanReviewWavedThrough, Actor: root.Token,
	})
	if err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	demandID, _, err := writer.Append(ctx, tx, events.Spec{
		SubjectKind: domain.SubjectWorkItem, SubjectID: item.ID,
		Kind: domain.EventDispatchRequested, Source: domain.SourceSystem,
		Payload: map[string]any{
			"work_item_id": item.ID, "capability": "review.complementary",
			"cultivar": "review.complementary", "origin_token_id": root.Token.ID,
		},
	})
	if err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	assignment, err := listenerSvc.ClaimDemand(ctx, reg.ID, listeners.ClaimDemandInput{
		DemandEventID: demandID, ObservedPolicyEventID: reg.PolicyEventID, Actor: principal.Token,
	})
	if err != nil {
		t.Fatal(err)
	}
	activationSvc := listeneractivation.NewService(pool, writer)
	s := New(Deps{
		Auth: authSvc, Access: access.NewService(pool), Idempotency: idempotency.NewMiddleware(pool, writer),
		WorkItems: workSvc, Listeners: listenerSvc, ListenerActivations: activationSvc, Feed: feed.NewService(pool),
	}, ServerInfo{Name: "meristem-test", Version: "test"}, nil)
	if err := s.Authenticate(ctx, principal.Secret); err != nil {
		t.Fatal(err)
	}

	isError, text := callToolForTest(t, s, "listeners.ensure_activation", map[string]any{
		"id": reg.ID.String(), "assignment_event_id": assignment.AssignmentEventID.String(),
		"binding_generation": "mcp-binding-v1", "idempotency_key": uuid.NewString(),
	})
	if isError {
		t.Fatalf("ensure: %s", text)
	}
	var ensured struct {
		Activation struct{ ID, StateEventID string } `json:"activation"`
	}
	if err := json.Unmarshal([]byte(text), &ensured); err != nil || ensured.Activation.ID == "" {
		t.Fatalf("decode ensure: %v (%s)", err, text)
	}

	isError, text = callToolForTest(t, s, "listener_activations.begin", map[string]any{
		"id": ensured.Activation.ID, "consumer_generation": "mcp-consumer-v1", "idempotency_key": uuid.NewString(),
	})
	if isError {
		t.Fatalf("begin: %s", text)
	}
	var begun struct {
		Action     string `json:"action"`
		Activation struct {
			StateEventID string `json:"state_event_id"`
		} `json:"activation"`
	}
	if err := json.Unmarshal([]byte(text), &begun); err != nil || begun.Action != "dispatch" {
		t.Fatalf("decode begin: %v (%s)", err, text)
	}

	isError, text = callToolForTest(t, s, "listener_activations.record_receipt", map[string]any{
		"id": ensured.Activation.ID, "observed_state_event_id": begun.Activation.StateEventID,
		"consumer_generation": "mcp-consumer-v1", "outcome": "completed",
		"reason": "turn_completed", "idempotency_key": uuid.NewString(),
	})
	if isError {
		t.Fatalf("receipt: %s", text)
	}
	var completed struct {
		Activation struct {
			State string `json:"state"`
		} `json:"activation"`
	}
	if err := json.Unmarshal([]byte(text), &completed); err != nil || completed.Activation.State != "completed" {
		t.Fatalf("decode receipt: %v (%s)", err, text)
	}
}
