package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/workitems"
)

func TestLocalAgentHTTPMaximumFeedWaitCompletesThroughAPIRouteIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	s := New(pool, nil)
	var deadlineMu sync.Mutex
	var deadlines []time.Time
	s.mcpSetWriteDeadline = func(_ http.ResponseWriter, got time.Time) error {
		deadlineMu.Lock()
		deadlines = append(deadlines, got)
		deadlineMu.Unlock()
		return nil
	}
	root, err := s.authService.CreateToken(ctx, auth.CreateTokenInput{Name: "mcp-wait-root", IsRoot: true, Source: domain.SourceHuman})
	if err != nil {
		t.Fatalf("create root: %v", err)
	}
	local, err := s.authService.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "mcp-wait-local",
		Source: domain.SourceAgent,
		Scopes: []string{access.ScopeMCPLocalAgentProfileV1, access.ScopeFeedRead},
		Actor:  &root.Token,
	})
	if err != nil {
		t.Fatalf("create local token: %v", err)
	}

	bootstrap := postLocalMCPTool(t, s, local.Secret, "feed.read", map[string]any{"bootstrap": "head"})
	var boot struct {
		NextCursor string `json:"next_cursor"`
	}
	if err := json.Unmarshal([]byte(bootstrap), &boot); err != nil || boot.NextCursor == "" {
		t.Fatalf("decode bootstrap cursor: %v payload=%s", err, bootstrap)
	}

	type waitResult struct {
		payload string
		err     error
	}
	resultCh := make(chan waitResult, 1)
	go func() {
		payload, err := doLocalMCPTool(s, local.Secret, "feed.read", map[string]any{
			"cursor": boot.NextCursor,
			"wait":   s.policy.MaxFeedWait.String(),
		})
		resultCh <- waitResult{payload: payload, err: err}
	}()
	time.Sleep(100 * time.Millisecond)
	if _, err := s.workItems.Create(ctx, workitems.CreateInput{Title: "wake maximum MCP feed wait", Actor: root.Token}); err != nil {
		t.Fatalf("append wake event: %v", err)
	}

	select {
	case result := <-resultCh:
		if result.err != nil || strings.Contains(result.payload, "wait exceeds server limit") || !strings.Contains(result.payload, "work_item.created") {
			t.Fatalf("maximum feed wait failed: %v payload=%s", result.err, result.payload)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("maximum feed wait did not return after a new event")
	}
	deadlineMu.Lock()
	gotDeadlines := append([]time.Time(nil), deadlines...)
	deadlineMu.Unlock()
	var latestSet time.Time
	clears := 0
	for _, deadline := range gotDeadlines {
		if deadline.IsZero() {
			clears++
			continue
		}
		if deadline.After(latestSet) {
			latestSet = deadline
		}
	}
	if latestSet.IsZero() || time.Until(latestSet) < s.policy.MaxFeedWait+3*time.Second {
		t.Fatalf("transport deadlines %v do not cover MaxFeedWait=%s plus response margin", gotDeadlines, s.policy.MaxFeedWait)
	}
	if clears != 2 {
		t.Fatalf("transport deadlines %v include %d clears, want one per MCP request", gotDeadlines, clears)
	}
}

func postLocalMCPTool(t *testing.T, s *Server, secret, name string, arguments map[string]any) string {
	t.Helper()
	payload, err := doLocalMCPTool(s, secret, name, arguments)
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func doLocalMCPTool(s *Server, secret, name string, arguments map[string]any) (string, error) {
	params, err := json.Marshal(map[string]any{"name": name, "arguments": arguments})
	if err != nil {
		return "", err
	}
	body := fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":%s}`, params)
	req := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(body))
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set("Authorization", "Bearer "+secret)
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		return "", fmt.Errorf("MCP status=%d body=%s", rec.Code, rec.Body.String())
	}
	var envelope struct {
		Error *struct {
			Message string `json:"message"`
		} `json:"error"`
		Result struct {
			IsError bool `json:"isError"`
			Content []struct {
				Text string `json:"text"`
			} `json:"content"`
		} `json:"result"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &envelope); err != nil {
		return "", fmt.Errorf("decode MCP response: %w body=%s", err, rec.Body.String())
	}
	if envelope.Error != nil || envelope.Result.IsError || len(envelope.Result.Content) == 0 {
		return "", fmt.Errorf("MCP call failed: %s", rec.Body.String())
	}
	return envelope.Result.Content[0].Text, nil
}
