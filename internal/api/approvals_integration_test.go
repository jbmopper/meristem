package api

import (
	"context"
	"net/http"
	"testing"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/approvals"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/workitems"
)

func TestApprovalsRESTLifecycleIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	authSvc := auth.NewService(pool, writer)
	rootResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "approval-root",
		IsRoot: true,
		Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create root token: %v", err)
	}
	root := rootResult.Token
	requester, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "approval-requester",
		Source: domain.SourceHuman,
		Scopes: []string{
			access.ScopeWorkItemsReadAll,
			access.ScopeWorkItemsWriteAll,
			access.ScopeApprovalsDecide,
		},
		Actor: &root,
	})
	if err != nil {
		t.Fatalf("create requester token: %v", err)
	}
	decider, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "approval-decider",
		Source: domain.SourceHuman,
		Scopes: []string{access.ScopeApprovalsDecide},
		Actor:  &root,
	})
	if err != nil {
		t.Fatalf("create decider token: %v", err)
	}
	systemResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "approval-system",
		Source: domain.SourceSystem,
		Actor:  &root,
	})
	if err != nil {
		t.Fatalf("create system token: %v", err)
	}

	workSvc := workitems.NewService(pool, writer)
	item, err := workSvc.Create(ctx, workitems.CreateInput{
		Title: "approval target",
		Actor: root,
	})
	if err != nil {
		t.Fatalf("create approval target: %v", err)
	}
	server := New(pool, nil)

	create := doREST(t, server.Handler(), http.MethodPost, "/v1/work-items/"+item.ID.String()+"/approvals", requester.Secret, "approval-create", []byte(`{
		"summary":"send outbound write",
		"request":{"connector":"http","method":"POST"},
		"expires_in_seconds":60
	}`))
	assertRESTStatus(t, create, http.StatusCreated)
	var createResp struct {
		Approval approvals.Approval `json:"approval"`
		Created  bool               `json:"created"`
		EventID  uuid.UUID          `json:"event_id"`
	}
	decodeResponse(t, create, &createResp)
	if !createResp.Created || createResp.Approval.ID == uuid.Nil || createResp.EventID == uuid.Nil {
		t.Fatalf("unexpected create response: %+v body=%s", createResp, create.Body.String())
	}
	if createResp.Approval.Status != approvals.StatusPending || createResp.Approval.WorkItemID != item.ID {
		t.Fatalf("unexpected created approval: %+v", createResp.Approval)
	}
	assertEventCount(t, pool, domain.EventApprovalCreated, 1)
	assertEventCount(t, pool, domain.EventApprovalDecided, 0)
	updated, err := workSvc.Get(ctx, item.ID)
	if err != nil {
		t.Fatalf("get awaiting item: %v", err)
	}
	if updated.State != domain.WorkItemAwaitingApproval {
		t.Fatalf("approval request did not park work item: state=%s", updated.State)
	}

	list := doREST(t, server.Handler(), http.MethodGet, "/v1/work-items/"+item.ID.String()+"/approvals", requester.Secret, "", nil)
	assertRESTStatus(t, list, http.StatusOK)
	var listResp struct {
		Items []approvals.Approval `json:"items"`
	}
	decodeResponse(t, list, &listResp)
	if len(listResp.Items) != 1 || listResp.Items[0].ID != createResp.Approval.ID {
		t.Fatalf("unexpected approval list: %+v", listResp.Items)
	}
	get := doREST(t, server.Handler(), http.MethodGet, "/v1/approvals/"+createResp.Approval.ID.String(), requester.Secret, "", nil)
	assertRESTStatus(t, get, http.StatusOK)

	sameToken := doREST(t, server.Handler(), http.MethodPost, "/v1/approvals/"+createResp.Approval.ID.String()+"/decision", requester.Secret, "approval-self-decide", []byte(`{"decision":"approved","reason":"same token"}`))
	assertRESTStatus(t, sameToken, http.StatusForbidden)
	assertErrorCode(t, sameToken, "separation_of_duties")
	assertEventCount(t, pool, domain.EventApprovalDecided, 0)

	decide := doREST(t, server.Handler(), http.MethodPost, "/v1/approvals/"+createResp.Approval.ID.String()+"/decision", decider.Secret, "approval-decide", []byte(`{"decision":"approved","reason":"owner approved"}`))
	assertRESTStatus(t, decide, http.StatusOK)
	var decideResp struct {
		Approval approvals.Approval `json:"approval"`
		Decided  bool               `json:"decided"`
		EventID  uuid.UUID          `json:"event_id"`
	}
	decodeResponse(t, decide, &decideResp)
	if !decideResp.Decided || decideResp.Approval.Status != approvals.StatusApproved {
		t.Fatalf("unexpected decision response: %+v body=%s", decideResp, decide.Body.String())
	}
	assertEventCount(t, pool, domain.EventApprovalDecided, 1)
	updated, err = workSvc.Get(ctx, item.ID)
	if err != nil {
		t.Fatalf("get approved item: %v", err)
	}
	if updated.State != domain.WorkItemRunning {
		t.Fatalf("approved approval did not resume work item: state=%s", updated.State)
	}

	expiring, err := workSvc.Create(ctx, workitems.CreateInput{
		Title: "approval expiry target",
		Actor: root,
	})
	if err != nil {
		t.Fatalf("create expiry target: %v", err)
	}
	approvalSvc := approvals.NewService(pool, writer)
	expiryCreate, err := approvalSvc.Create(ctx, approvals.CreateInput{
		WorkItemID: expiring.ID,
		Summary:    "expire me",
		Actor:      requester.Token,
	})
	if err != nil {
		t.Fatalf("create expiring approval: %v", err)
	}
	expired, err := approvalSvc.Expire(ctx, approvals.ExpireInput{
		ApprovalID: expiryCreate.Approval.ID,
		Reason:     "clock elapsed",
		Actor:      systemResult.Token,
	})
	if err != nil {
		t.Fatalf("expire approval: %v", err)
	}
	if expired.Approval.Status != approvals.StatusExpired {
		t.Fatalf("expired approval status = %s", expired.Approval.Status)
	}
	assertEventCount(t, pool, domain.EventApprovalExpired, 1)
	expiring, err = workSvc.Get(ctx, expiring.ID)
	if err != nil {
		t.Fatalf("get expired item: %v", err)
	}
	if expiring.State != domain.WorkItemBlocked {
		t.Fatalf("expired approval should block work item, got %s", expiring.State)
	}
}
