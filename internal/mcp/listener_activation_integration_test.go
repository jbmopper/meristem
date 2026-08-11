package mcp

import (
	"context"
	"encoding/json"
	"strings"
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
	task, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name: "activation-mcp-task", Source: domain.SourceAgent,
		Scopes: []string{access.ScopeMCPListenerTaskProfileV1}, Actor: &root.Token,
	})
	if err != nil {
		t.Fatal(err)
	}
	otherTask, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name: "activation-mcp-other-task", Source: domain.SourceAgent,
		Scopes: []string{access.ScopeMCPListenerTaskProfileV1}, Actor: &root.Token,
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

	binding, err := listeneractivation.TaskBindingGeneration("mcp-binding-v1", "meristem-git-v1", task.Token.ID)
	if err != nil {
		t.Fatal(err)
	}
	isError, text := callToolForTest(t, s, "listeners.ensure_activation", map[string]any{
		"id": reg.ID.String(), "assignment_event_id": assignment.AssignmentEventID.String(),
		"binding_generation": binding, "task_principal_token_id": task.Token.ID.String(),
		"idempotency_key": uuid.NewString(),
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

	activationID := uuid.MustParse(ensured.Activation.ID)
	taskBinding := ListenerTaskBinding{
		ActivationID: activationID, WorkItemID: item.ID,
		AssignmentEventID: assignment.AssignmentEventID, ExpectedActorID: task.Token.ID,
	}
	taskServer := New(Deps{
		Auth: authSvc, Access: access.NewService(pool), Idempotency: idempotency.NewMiddleware(pool, writer),
		WorkItems: workSvc, Listeners: listenerSvc, ListenerActivations: activationSvc, Feed: feed.NewService(pool),
	}, ServerInfo{Name: "meristem", Version: "test"}, nil)
	if err := taskServer.AuthenticateListenerTask(ctx, task.Secret, taskBinding); err != nil {
		t.Fatalf("authenticate bound task: %v", err)
	}
	listed, rerr := taskServer.gatedToolsList(taskServer.actorToken())
	if rerr != nil {
		t.Fatalf("task tools list: %+v", rerr)
	}
	tools := listed.(map[string]any)["tools"].([]httpToolDescriptor)
	if len(tools) != 3 {
		t.Fatalf("task tool count=%d tools=%+v", len(tools), tools)
	}
	wantTools := map[string]bool{"work_items.get": true, "work_items.get_assignment": true, "work_items.append_event": true}
	for _, tool := range tools {
		if !wantTools[tool.Name] {
			t.Fatalf("unexpected listener task tool %q", tool.Name)
		}
		delete(wantTools, tool.Name)
	}
	if len(wantTools) != 0 {
		t.Fatalf("missing listener task tools: %v", wantTools)
	}
	isError, text = callToolForTest(t, taskServer, "work_items.get_assignment", map[string]any{"id": item.ID.String()})
	if isError || !strings.Contains(text, assignment.AssignmentEventID.String()) {
		t.Fatalf("task get_assignment error=%v text=%s", isError, text)
	}
	isError, text = callToolForTest(t, taskServer, "work_items.append_event", map[string]any{
		"id": item.ID.String(), "kind": "agent.listener_task_progress",
		"payload": map[string]any{"status": "task-bound"}, "idempotency_key": uuid.NewString(),
	})
	if isError {
		t.Fatalf("task append_event: %s", text)
	}
	var appendedActor uuid.UUID
	if err := pool.QueryRow(ctx, `
		SELECT actor_token_id FROM events
		WHERE subject_id=$1 AND kind=$2 AND payload->>'inner_kind'='agent.listener_task_progress'
	`, item.ID, domain.EventWorkItemEventAppended).Scan(&appendedActor); err != nil {
		t.Fatalf("read task-attributed event: %v", err)
	}
	if appendedActor != task.Token.ID {
		t.Fatalf("task event actor=%s want=%s", appendedActor, task.Token.ID)
	}
	hidden := roundtrip(t, taskServer, `{"jsonrpc":"2.0","id":11,"method":"tools/call","params":{"name":"work_items.transition","arguments":{"id":"`+item.ID.String()+`","to":"done","reason":"must stay hidden","idempotency_key":"`+uuid.NewString()+`"}}}`)
	if hidden.Error == nil || !strings.Contains(hidden.Error.Message, "tool not enabled") {
		t.Fatalf("hidden task transition response=%+v", hidden)
	}
	ordinary := New(Deps{Auth: authSvc}, ServerInfo{Name: "meristem", Version: "test"}, nil)
	if err := ordinary.Authenticate(ctx, task.Secret); err == nil {
		t.Fatal("marker-only listener task credential authenticated outside assignment-bound exchange")
	}
	wrongActor := taskBinding
	wrongActor.ExpectedActorID = uuid.New()
	if err := New(Deps{Auth: authSvc, ListenerActivations: activationSvc}, ServerInfo{}, nil).AuthenticateListenerTask(ctx, task.Secret, wrongActor); err == nil {
		t.Fatal("wrong expected task actor unexpectedly authenticated")
	}
	if err := New(Deps{Auth: authSvc, ListenerActivations: activationSvc}, ServerInfo{}, nil).AuthenticateListenerTask(ctx, otherTask.Secret, taskBinding); err == nil {
		t.Fatal("swapped valid marker-only task credential unexpectedly authenticated")
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
	if _, rerr := taskServer.gatedToolsList(taskServer.actorToken()); rerr == nil {
		t.Fatal("completed activation retained listener task MCP authority")
	}
}
