package api

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/listeners"
	"github.com/jbmopper/meristem/internal/workitems"
)

func TestListenerActivationRESTLifecycle(t *testing.T) {
	f := newListenerFixture(t)
	h := f.server.Handler()
	listenerSvc := listeners.NewService(f.pool, f.writer)
	reg, err := listenerSvc.Register(f.ctx, listeners.RegisterInput{
		Name: "activation-rest", PrincipalTokenID: f.principal.Token.ID,
		Provider: "codex", Capabilities: []string{"review.complementary"}, Actor: f.admin.Token,
	})
	if err != nil {
		t.Fatal(err)
	}
	reg, err = listenerSvc.SetPolicy(f.ctx, reg.ID, listeners.SetPolicyInput{
		Policy: listeners.Policy{Capabilities: []string{"review.complementary"}, MaxConcurrentAssignments: 1}, Actor: f.admin.Token,
	})
	if err != nil {
		t.Fatal(err)
	}
	work := workitems.NewService(f.pool, f.writer)
	item, err := work.Create(f.ctx, workitems.CreateInput{
		Title: "activation REST demand", State: domain.WorkItemPlanned,
		SuggestedConvergenceChecks: []string{"event:activation.completed"},
		HumanReviewStatus:          domain.HumanReviewWavedThrough, Actor: f.root.Token,
	})
	if err != nil {
		t.Fatal(err)
	}
	tx, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	demandID, _, err := f.writer.Append(f.ctx, tx, events.Spec{
		SubjectKind: domain.SubjectWorkItem, SubjectID: item.ID,
		Kind: domain.EventDispatchRequested, Source: domain.SourceSystem,
		Payload: map[string]any{
			"work_item_id": item.ID, "state": item.State,
			"state_entered_at_unix": item.StateEnteredAt.Unix(),
			"capability":            "review.complementary",
			"cultivar":              "review.complementary", "origin_token_id": f.root.Token.ID,
		},
	})
	if err != nil {
		_ = tx.Rollback(f.ctx)
		t.Fatal(err)
	}
	if err := tx.Commit(f.ctx); err != nil {
		t.Fatal(err)
	}
	assignment, err := listenerSvc.ClaimDemand(f.ctx, reg.ID, listeners.ClaimDemandInput{
		DemandEventID: demandID, ObservedPolicyEventID: reg.PolicyEventID, Actor: f.principal.Token,
	})
	if err != nil {
		t.Fatal(err)
	}

	ensureBody, _ := json.Marshal(map[string]any{
		"assignment_event_id": assignment.AssignmentEventID,
		"binding_generation":  "rest-binding-v1", "attempt": 1,
	})
	rec := doREST(t, h, http.MethodPost, "/v1/listeners/"+reg.ID.String()+"/activations/ensure", f.root.Secret, uuid.NewString(), ensureBody)
	assertRESTStatus(t, rec, http.StatusForbidden)
	rec = doREST(t, h, http.MethodPost, "/v1/listeners/"+reg.ID.String()+"/activations/ensure", f.principal.Secret, uuid.NewString(), ensureBody)
	assertRESTStatus(t, rec, http.StatusOK)
	var ensured struct {
		Activation struct{ ID, State, StateEventID string } `json:"activation"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &ensured); err != nil {
		t.Fatal(err)
	}
	if ensured.Activation.ID == "" || ensured.Activation.State != "requested" {
		t.Fatalf("ensure response=%s", rec.Body.String())
	}

	beginBody := []byte(`{"consumer_generation":"rest-consumer-v1"}`)
	rec = doREST(t, h, http.MethodPost, "/v1/listener-activations/"+ensured.Activation.ID+"/begin", f.principal.Secret, uuid.NewString(), beginBody)
	assertRESTStatus(t, rec, http.StatusOK)
	var begun struct {
		Action     string `json:"action"`
		Activation struct {
			StateEventID string `json:"state_event_id"`
		} `json:"activation"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &begun); err != nil {
		t.Fatal(err)
	}
	if begun.Action != "dispatch" || begun.Activation.StateEventID == "" {
		t.Fatalf("begin response=%s", rec.Body.String())
	}

	receiptBody, _ := json.Marshal(map[string]any{
		"observed_state_event_id": begun.Activation.StateEventID,
		"consumer_generation":     "rest-consumer-v1",
		"outcome":                 "completed", "reason": "turn_completed",
	})
	rec = doREST(t, h, http.MethodPost, "/v1/listener-activations/"+ensured.Activation.ID+"/receipts", f.principal.Secret, uuid.NewString(), receiptBody)
	assertRESTStatus(t, rec, http.StatusOK)
	var completed struct {
		Activation struct {
			State string `json:"state"`
		} `json:"activation"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &completed); err != nil {
		t.Fatal(err)
	}
	if completed.Activation.State != "completed" {
		t.Fatalf("completion response=%s", rec.Body.String())
	}
}
