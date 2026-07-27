package main

// The Watch Ergonomics acceptance bar, end to end against the real server,
// through the production code path only: the watcher itself bootstraps its
// identity-bound cursor (no test-side seeding), holds one SSE connection
// under a named projection plus predicates, and a filtered event appended
// AFTER the watcher started wakes the --exec hook with no cron polling.
// The same run then pins cross-identity refusal through the CLI path: a
// cursor persisted under this lens is rejected loudly when the watcher is
// restarted with a different lens and no reset opt-in.

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
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
	"github.com/jbmopper/meristem/internal/feed"
	"github.com/jbmopper/meristem/internal/projectiondefs"
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
	if _, _, err := projectiondefs.NewService(pool, writer).Define(ctx, root.Token, projectiondefs.DefineInput{
		Name:    "watch-e2e-lens",
		Version: 1,
		Type:    projectiondefs.ProjectionTypeFeed,
		Filter:  feed.ProjectionFilter{Kinds: []string{domain.EventWorkItemEventAppended}},
	}); err != nil {
		t.Fatalf("define projection: %v", err)
	}

	httpSrv := httptest.NewServer(api.New(pool, nil).Handler())
	defer httpSrv.Close()

	lens := buildFeedQuery(feedQueryFlags{
		projection:    "watch-e2e-lens",
		scope:         "assigned",
		excludeActors: []string{"self"},
	})
	client := &feedClient{
		baseURL:    httpSrv.URL,
		token:      watcher.Secret,
		query:      lens,
		http:       &http.Client{Timeout: 30 * time.Second},
		streamHTTP: &http.Client{Timeout: 0},
	}

	dir := t.TempDir()
	cursorFile := filepath.Join(dir, "cursor")
	hookOut := filepath.Join(dir, "woken")

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

	// The watcher's own bootstrap must persist an identity-bound cursor
	// before we let the wake event exist — this is the production path the
	// review demanded, not test-side seeding.
	var bootCursor string
	deadline := time.After(10 * time.Second)
	for bootCursor == "" {
		bootCursor, _ = loadCursorFile(cursorFile)
		if bootCursor != "" {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("watcher never bootstrapped a cursor")
		case <-time.After(20 * time.Millisecond):
		}
	}

	// The wake event arrives only after the watcher is durable.
	if err := workSvc.AppendEvent(ctx, item.ID, "agent.watch_e2e_test",
		map[string]any{"marker": "watch-e2e-wake", "addressee_token_id": watcher.Token.ID}, root.Token); err != nil {
		t.Fatalf("append wake event: %v", err)
	}

	deadline = time.After(15 * time.Second)
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
	deadline = time.After(5 * time.Second)
	for {
		cursor, err := loadCursorFile(cursorFile)
		if err != nil {
			t.Fatalf("load cursor: %v", err)
		}
		if cursor != "" && cursor != bootCursor {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("cursor did not durably advance past bootstrap %q", bootCursor)
		case <-time.After(25 * time.Millisecond):
		}
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("watch exited with error: %v", err)
	}

	// Cross-identity refusal through the CLI path: the persisted cursor was
	// minted under the projection lens; restarting the watcher WITHOUT the
	// projection is a different identity and must exit loudly, preserving
	// the cursor file, because no reset was opted into.
	mismatchClient := &feedClient{
		baseURL:    httpSrv.URL,
		token:      watcher.Secret,
		query:      buildFeedQuery(feedQueryFlags{scope: "assigned", excludeActors: []string{"self"}}),
		http:       &http.Client{Timeout: 30 * time.Second},
		streamHTTP: &http.Client{Timeout: 0},
	}
	preserved, _ := loadCursorFile(cursorFile)
	mismatchCtx, cancelMismatch := context.WithTimeout(ctx, 10*time.Second)
	defer cancelMismatch()
	err = runFeedWatch(mismatchCtx, nil, mismatchClient, watchOptions{
		retryBackoff: 30 * time.Second,
		cursorFile:   cursorFile,
	}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "mismatch") {
		t.Fatalf("cross-identity restart should refuse loudly, got err=%v", err)
	}
	if after, _ := loadCursorFile(cursorFile); after != preserved {
		t.Fatalf("refusal must preserve the cursor file: had %q, now %q", preserved, after)
	}

	// Same-identity restart resumes cleanly from the preserved cursor.
	resumeCtx, cancelResume := context.WithTimeout(ctx, 5*time.Second)
	defer cancelResume()
	if err := runFeedWatch(resumeCtx, nil, client, watchOptions{
		retryBackoff: 30 * time.Second,
		cursorFile:   cursorFile,
	}, io.Discard); err != nil {
		t.Fatalf("same-identity resume should stream until ctx timeout without error, got %v", err)
	}
}
