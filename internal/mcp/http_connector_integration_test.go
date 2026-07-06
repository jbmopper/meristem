package mcp

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/approvals"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/httpconnector"
	"github.com/jbmopper/meristem/internal/idempotency"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/workitems"
)

func TestHTTPConnectorMCPWriteCreatesApprovalWithoutOutboundRequest(t *testing.T) {
	ctx := context.Background()
	pool := newMCPIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	authSvc := auth.NewService(pool, writer)
	rootResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "mcp-http-connector-root",
		IsRoot: true,
		Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create root token: %v", err)
	}
	root := rootResult.Token
	workSvc := workitems.NewService(pool, writer)
	item, err := workSvc.Create(ctx, workitems.CreateInput{
		Title: "mcp connector target",
		Actor: root,
	})
	if err != nil {
		t.Fatalf("create target: %v", err)
	}
	requester, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "mcp-http-connector-requester",
		Source: domain.SourceAgent,
		Scopes: []string{
			access.ScopeWorkItemsRead,
			access.ScopeWorkItemsWrite,
			access.ScopeFeedReadAssigned,
			"work_items.tree:" + item.ID.String(),
		},
		Actor: &root,
	})
	if err != nil {
		t.Fatalf("create requester token: %v", err)
	}

	var hits atomic.Int32
	client := roundTripDoer{roundTrip: func() {
		hits.Add(1)
	}}
	approvalSvc := approvals.NewService(pool, writer)
	s := New(Deps{
		Auth:          authSvc,
		Access:        access.NewService(pool),
		Idempotency:   idempotency.NewMiddleware(pool, writer),
		WorkItems:     workSvc,
		Approvals:     approvalSvc,
		HTTPConnector: httpconnector.NewService(pool, writer, approvalSvc, client),
	}, ServerInfo{Name: "meristem-test", Version: "test"}, nil)
	if err := s.Authenticate(ctx, requester.Secret); err != nil {
		t.Fatalf("authenticate requester: %v", err)
	}

	isError, text := callToolForTest(t, s, "connectors.http_request", map[string]any{
		"work_item_id":    item.ID.String(),
		"mode":            "write",
		"method":          "POST",
		"url":             "https://example.invalid/write",
		"body":            map[string]any{"hello": "world"},
		"idempotency_key": "mcp-http-connector-write",
	})
	if isError {
		t.Fatalf("connectors.http_request returned error: %s", text)
	}
	if !strings.Contains(text, `"approval"`) || !strings.Contains(text, `"awaiting_approval"`) {
		t.Fatalf("tool response did not include approval-gated action: %s", text)
	}
	if got := hits.Load(); got != 0 {
		t.Fatalf("write-mode MCP connector executed outbound request before approval: hits=%d", got)
	}
	if got := eventCount(t, pool, domain.EventHTTPConnectorActionRequested); got != 1 {
		t.Fatalf("http_connector.action_requested count = %d, want 1", got)
	}
	if got := eventCount(t, pool, domain.EventApprovalCreated); got != 1 {
		t.Fatalf("approval.created count = %d, want 1", got)
	}
	if got := eventCount(t, pool, domain.EventHTTPConnectorActionSent); got != 0 {
		t.Fatalf("http_connector.action_sent count = %d, want 0", got)
	}
}

type roundTripDoer struct {
	roundTrip func()
}

func (d roundTripDoer) Do(req *http.Request) (*http.Response, error) {
	d.roundTrip()
	return &http.Response{
		StatusCode: http.StatusAccepted,
		Body:       io.NopCloser(strings.NewReader(`{"unexpected":true}`)),
		Header:     make(http.Header),
		Request:    req,
	}, nil
}
