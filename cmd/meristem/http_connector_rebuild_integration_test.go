package main

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/approvals"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/httpconnector"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/workitems"
)

func TestHTTPConnectorRebuildFromEvents(t *testing.T) {
	ctx := context.Background()
	pool := newCmdIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	authSvc := auth.NewService(pool, writer)
	rootResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "http-connector-rebuild-root",
		IsRoot: true,
		Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create root token: %v", err)
	}
	root := rootResult.Token
	requester, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "http-connector-rebuild-requester",
		Source: domain.SourceAgent,
		Actor:  &root,
	})
	if err != nil {
		t.Fatalf("create requester token: %v", err)
	}
	decider, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "http-connector-rebuild-decider",
		Source: domain.SourceHuman,
		Actor:  &root,
	})
	if err != nil {
		t.Fatalf("create decider token: %v", err)
	}

	workSvc := workitems.NewService(pool, writer)
	item, err := workSvc.Create(ctx, workitems.CreateInput{Title: "http connector rebuild", Actor: root})
	if err != nil {
		t.Fatalf("create work item: %v", err)
	}
	approvalSvc := approvals.NewService(pool, writer)
	connectorSvc := httpconnector.NewService(pool, writer, approvalSvc, responseDoer{})
	requested, err := connectorSvc.Request(ctx, httpconnector.RequestInput{
		WorkItemID: item.ID,
		Mode:       httpconnector.ModeWrite,
		Method:     http.MethodPost,
		URL:        "https://example.invalid/rebuild",
		Body:       []byte(`{"hello":"rebuild"}`),
		Actor:      requester.Token,
	})
	if err != nil {
		t.Fatalf("request connector write: %v", err)
	}
	if requested.Approval == nil {
		t.Fatalf("write request did not create approval: %+v", requested)
	}
	if _, err := approvalSvc.Decide(ctx, approvals.DecisionInput{
		ApprovalID: requested.Approval.ID,
		Decision:   approvals.DecisionApproved,
		Reason:     "rebuild approval",
		Actor:      decider.Token,
	}); err != nil {
		t.Fatalf("approve connector write: %v", err)
	}
	if _, err := connectorSvc.EnqueueApprovedWrite(ctx, requested.Action.ID, decider.Token); err != nil {
		t.Fatalf("enqueue connector write: %v", err)
	}
	if _, err := connectorSvc.DispatchOnce(ctx, decider.Token, 0); err != nil {
		t.Fatalf("dispatch connector write: %v", err)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	report, err := rebuildAndDiff(ctx, pool, app.NewProjectionRegistry(), "http_connector_rebuild", logger, false)
	if err != nil {
		t.Fatalf("rebuild http connector projection: %v", err)
	}
	if len(report.mismatches) != 0 {
		t.Fatalf("http connector rebuild had mismatches: %+v", report.mismatches)
	}
}

type responseDoer struct{}

func (responseDoer) Do(req *http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusAccepted,
		Body:       io.NopCloser(strings.NewReader(`{"accepted":true}`)),
		Header:     make(http.Header),
		Request:    req,
	}, nil
}
