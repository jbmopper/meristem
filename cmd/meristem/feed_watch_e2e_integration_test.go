package main

// The Watch Ergonomics acceptance bar, end to end against the real server:
// a filtered event delivered over the SSE channel wakes a task (the --exec
// hook) with no cron polling anywhere in the client loop. The watcher holds
// one long-lived connection; the only timers involved are reconnect backoff
// (never reached on the healthy path) and this test's own deadlines.

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/api"
	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/workitems"
)

func TestWatchWakeEndToEndIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newCmdIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	writer := app.NewEventWriter()
	authSvc := auth.NewService(pool, writer)
	root, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "watch-e2e-root", IsRoot: true, Source: domain.SourceHuman})
	if err != nil {
		t.Fatalf("create root: %v", err)
	}
	workSvc := workitems.NewService(pool, writer)
	tree, err := workSvc.Create(ctx, workitems.CreateInput{Title: "watch-e2e-tree", Actor: root.Token})
	if err != nil {
		t.Fatalf("create tree: %v", err)
	}
	item, err := workSvc.SpawnChild(ctx, tree.ID, workitems.CreateInput{Title: "watch-e2e-item", Actor: root.Token})
	if err != nil {
		t.Fatalf("spawn item: %v", err)
	}
	watcher, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name: "watch-e2e-watcher", Source: domain.SourceAgent,
		Scopes: []string{access.ScopeWorkItemsRead, access.ScopeFeedReadAssigned, "work_items.tree:" + tree.ID.String()},
		Actor:  &root.Token,
	})
	if err != nil {
		t.Fatalf("create watcher token: %v", err)
	}
	if _, err := workSvc.Claim(ctx, item.ID, watcher.Token); err != nil {
		t.Fatalf("claim item: %v", err)
	}

	httpSrv := httptest.NewServer(api.New(pool, nil).Handler())
	defer httpSrv.Close()

	// Mint the channel cursor BEFORE the wake event exists, through the page
	// surface, under exactly the lens the watcher will hold. Seeding the
	// cursor file with it makes delivery deterministic (no from-now race)
	// and proves the cursor is portable from page to stream — same filter,
	// same fingerprint identity.
	lens := url.Values{"scope": {"assigned"}, "exclude_actor": {"self"}}
	pageURL := httpSrv.URL + "/v1/feed?wait=0s&" + lens.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, pageURL, nil)
	if err != nil {
		t.Fatalf("cursor request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+watcher.Secret)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("mint cursor: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("mint cursor: status %d body=%s", resp.StatusCode, body)
	}
	var page struct {
		NextCursor string `json:"next_cursor"`
	}
	if err := json.Unmarshal(body, &page); err != nil || page.NextCursor == "" {
		t.Fatalf("decode cursor: err=%v body=%s", err, body)
	}

	dir := t.TempDir()
	cursorFile := filepath.Join(dir, "cursor")
	hookOut := filepath.Join(dir, "woken")
	if err := saveCursorFile(cursorFile, page.NextCursor); err != nil {
		t.Fatalf("seed cursor file: %v", err)
	}

	// The wake event: appended by root, addressed to the watcher, before the
	// watcher even connects — replay-from-cursor must deliver it.
	if err := workSvc.AppendEvent(ctx, item.ID, "agent.watch_e2e_test",
		map[string]any{"marker": "watch-e2e-wake", "addressee_token_id": watcher.Token.ID}, root.Token); err != nil {
		t.Fatalf("append wake event: %v", err)
	}

	client := &feedClient{
		baseURL:    httpSrv.URL,
		token:      watcher.Secret,
		query:      lens,
		http:       &http.Client{Timeout: 30 * time.Second},
		streamHTTP: &http.Client{Timeout: 0},
	}
	watchCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- runFeedWatch(watchCtx, nil, client, watchOptions{
			retryBackoff: 30 * time.Second, // never reached on the healthy path
			cursorFile:   cursorFile,
			execCmd:      "cat >> " + hookOut,
			ndjson:       true,
		}, io.Discard)
	}()

	deadline := time.After(15 * time.Second)
	for {
		data, _ := os.ReadFile(hookOut)
		if strings.Contains(string(data), "watch-e2e-wake") {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("the filtered event never woke the hook; hook file: %q", string(data))
		case <-time.After(25 * time.Millisecond):
		}
	}

	// The durable cursor advanced past the delivered wake.
	cursor, err := loadCursorFile(cursorFile)
	if err != nil || cursor == "" || cursor == page.NextCursor {
		t.Fatalf("cursor did not durably advance: %q (seeded %q) err=%v", cursor, page.NextCursor, err)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("watch exited with error: %v", err)
	}
}
