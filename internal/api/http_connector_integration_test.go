package api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/approvals"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/httpconnector"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/workitems"
)

func TestHTTPConnectorWriteRequiresApprovalBeforeDispatch(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	var hits atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"accepted":true}`))
	}))
	defer upstream.Close()

	writer := app.NewEventWriter()
	authSvc := auth.NewService(pool, writer)
	rootResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "http-connector-root",
		IsRoot: true,
		Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create root token: %v", err)
	}
	root := rootResult.Token
	requester, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "http-connector-requester",
		Source: domain.SourceAgent,
		Scopes: []string{
			access.ScopeWorkItemsReadAll,
			access.ScopeWorkItemsWriteAll,
		},
		Actor: &root,
	})
	if err != nil {
		t.Fatalf("create requester token: %v", err)
	}
	decider, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "http-connector-decider",
		Source: domain.SourceHuman,
		Scopes: []string{access.ScopeApprovalsDecide},
		Actor:  &root,
	})
	if err != nil {
		t.Fatalf("create decider token: %v", err)
	}

	workSvc := workitems.NewService(pool, writer)
	item, err := workSvc.Create(ctx, workitems.CreateInput{
		Title: "approval-gated connector target",
		Actor: root,
	})
	if err != nil {
		t.Fatalf("create work item: %v", err)
	}
	server := New(pool, nil)
	rec := doREST(t, server.Handler(), http.MethodPost, "/v1/work-items/"+item.ID.String()+"/http-connector/actions", requester.Secret, "http-connector-write", []byte(`{
		"mode":"write",
		"method":"POST",
		"url":"`+upstream.URL+`",
		"body":{"hello":"world"}
	}`))
	assertRESTStatus(t, rec, http.StatusCreated)
	var created struct {
		Action          httpconnector.Action `json:"action"`
		Approval        approvals.Approval   `json:"approval"`
		Created         bool                 `json:"created"`
		EventID         uuid.UUID            `json:"event_id"`
		ApprovalEventID uuid.UUID            `json:"approval_event_id"`
	}
	decodeResponse(t, rec, &created)
	if !created.Created || created.EventID == uuid.Nil || created.ApprovalEventID == uuid.Nil {
		t.Fatalf("unexpected create response: %+v body=%s", created, rec.Body.String())
	}
	if created.Action.Status != httpconnector.StatusAwaitingApproval || created.Action.ApprovalID == nil {
		t.Fatalf("write action should be awaiting approval: %+v", created.Action)
	}
	if created.Approval.Status != approvals.StatusPending {
		t.Fatalf("approval status = %s, want pending", created.Approval.Status)
	}
	if got := hits.Load(); got != 0 {
		t.Fatalf("write request executed before approval: hits=%d", got)
	}
	assertEventCount(t, pool, domain.EventHTTPConnectorActionRequested, 1)
	assertEventCount(t, pool, domain.EventHTTPConnectorActionApproved, 0)
	assertEventCount(t, pool, domain.EventHTTPConnectorActionSent, 0)
	assertTableCount(t, pool, "outbox_events", 0)

	decide := doREST(t, server.Handler(), http.MethodPost, "/v1/approvals/"+created.Approval.ID.String()+"/decision", decider.Secret, "http-connector-approve", []byte(`{"decision":"approved","reason":"integration test"}`))
	assertRESTStatus(t, decide, http.StatusOK)
	if got := hits.Load(); got != 0 {
		t.Fatalf("approval decision executed connector before enqueue: hits=%d", got)
	}

	enqueued, err := server.httpConnector.EnqueueApprovedWrite(ctx, created.Action.ID, decider.Token)
	if err != nil {
		t.Fatalf("enqueue approved write: %v", err)
	}
	if !enqueued.Fresh || enqueued.Action.Status != httpconnector.StatusApproved {
		t.Fatalf("unexpected enqueue result: %+v", enqueued)
	}
	assertEventCount(t, pool, domain.EventHTTPConnectorActionApproved, 1)
	assertTableCount(t, pool, "outbox_events", 1)
	if got := hits.Load(); got != 0 {
		t.Fatalf("enqueue executed connector before dispatch: hits=%d", got)
	}

	dispatched, err := server.httpConnector.DispatchOnce(ctx, decider.Token, time.Minute)
	if err != nil {
		t.Fatalf("dispatch once: %v", err)
	}
	if !dispatched.Dispatched || dispatched.HTTPStatus != http.StatusAccepted {
		t.Fatalf("unexpected dispatch result: %+v", dispatched)
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("dispatch hit count = %d, want 1", got)
	}
	assertEventCount(t, pool, domain.EventHTTPConnectorActionSent, 1)
	if dispatched.Action.Status != httpconnector.StatusSent || dispatched.Action.ResponseStatus == nil || *dispatched.Action.ResponseStatus != http.StatusAccepted {
		t.Fatalf("sent action not projected correctly: %+v", dispatched.Action)
	}

	_, err = server.httpConnector.DispatchOnce(ctx, decider.Token, time.Minute)
	if !errors.Is(err, httpconnector.ErrNoPendingOutbox) {
		t.Fatalf("second dispatch error = %v, want ErrNoPendingOutbox", err)
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("second dispatch should not repeat write: hits=%d", got)
	}
}
