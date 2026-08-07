package main

// Slice-3 exit criteria, end to end against the real server through the
// production supervisor code path: restart in IDLE and in FOCUSED loses no
// open demand and accepts no duplicate assignment; candidates are attempted
// in deterministic order; after release the supervisor restores the LATEST
// base policy (revision-specific cursors); the control lane interrupts a
// streaming idle phase; the focus cursor is discarded only after the release
// is observed.

import (
	"context"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/api"
	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/listeners"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/workitems"
)

type supervisorFixture struct {
	ctx       context.Context
	pool      *pgxpool.Pool
	writer    *events.Writer
	workSvc   *workitems.Service
	listeners *listeners.Service
	tree      domain.WorkItem
	reg       listeners.Registration
	principal auth.CreateTokenResult
	admin     auth.CreateTokenResult
	root      auth.CreateTokenResult
	server    *httptest.Server
	sup       *listenerSupervisor
}

func newSupervisorFixture(t *testing.T, name string) *supervisorFixture {
	t.Helper()
	ctx := context.Background()
	pool := newCmdIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	writer := app.NewEventWriter()
	authSvc := auth.NewService(pool, writer)
	root, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: name + "-root", IsRoot: true, Source: domain.SourceHuman})
	if err != nil {
		t.Fatalf("create root: %v", err)
	}
	system, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: name + "-system", Source: domain.SourceSystem, Actor: &root.Token})
	if err != nil {
		t.Fatalf("create system: %v", err)
	}
	if _, _, err := seedProjectionFixtures(ctx, pool, writer, system.Token); err != nil {
		t.Fatalf("seed projections: %v", err)
	}
	admin, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name: name + "-admin", Source: domain.SourceHuman,
		Scopes: []string{access.ScopeListenersAdmin}, Actor: &root.Token,
	})
	if err != nil {
		t.Fatalf("create admin: %v", err)
	}
	workSvc := workitems.NewService(pool, writer)
	tree, err := workSvc.Create(ctx, workitems.CreateInput{Title: name + "-tree", Actor: root.Token})
	if err != nil {
		t.Fatalf("create tree: %v", err)
	}
	principal, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name: name + "-principal", Source: domain.SourceAgent,
		Scopes: []string{
			access.ScopeWorkItemsRead, access.ScopeWorkItemsWrite,
			access.ScopeFeedRead, "work_items.tree:" + tree.ID.String(),
		},
		Actor: &root.Token,
	})
	if err != nil {
		t.Fatalf("create principal: %v", err)
	}
	listenerSvc := listeners.NewService(pool, writer)
	reg, err := listenerSvc.Register(ctx, listeners.RegisterInput{
		Name: name, PrincipalTokenID: principal.Token.ID,
		Provider: "test", Capabilities: []string{"review.complementary"}, Actor: admin.Token,
	})
	if err != nil {
		t.Fatalf("register listener: %v", err)
	}
	if _, err := listenerSvc.SetPolicy(ctx, reg.ID, listeners.SetPolicyInput{
		Policy: listeners.Policy{Capabilities: []string{"review.complementary"}, MaxConcurrentAssignments: 1},
		Actor:  admin.Token,
	}); err != nil {
		t.Fatalf("set base policy: %v", err)
	}
	server := httptest.NewServer(api.New(pool, nil).Handler())
	t.Cleanup(server.Close)
	f := &supervisorFixture{
		ctx: ctx, pool: pool, writer: writer, workSvc: workSvc, listeners: listenerSvc,
		tree: tree, principal: principal, admin: admin, root: root, server: server,
	}
	f.reg, err = listenerSvc.Get(ctx, reg.ID)
	if err != nil {
		t.Fatalf("re-read registration: %v", err)
	}
	f.sup = &listenerSupervisor{
		api:       server.URL,
		token:     principal.Secret,
		name:      name,
		cursorDir: t.TempDir(),
		backoff:   100 * time.Millisecond,
		logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		http:      f.server.Client(),
	}
	return f
}

// spawnDemand creates a claimable item in the fixture tree and appends its
// durable dispatch demand the way the reconciler does.
func (f *supervisorFixture) spawnDemand(t *testing.T, title string) domain.WorkItem {
	t.Helper()
	item, err := f.workSvc.SpawnChild(f.ctx, f.tree.ID, workitems.CreateInput{
		Title: title, State: domain.WorkItemRunning,
		SuggestedConvergenceChecks: []string{"event:supervisor-fixture"},
		HumanReviewStatus:          domain.HumanReviewWavedThrough,
		Actor:                      f.root.Token,
	})
	if err != nil {
		t.Fatalf("spawn %s: %v", title, err)
	}
	tx, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatalf("begin demand tx: %v", err)
	}
	defer func() { _ = tx.Rollback(f.ctx) }()
	if _, _, err := f.writer.Append(f.ctx, tx, events.Spec{
		SubjectKind: domain.SubjectWorkItem,
		SubjectID:   item.ID,
		Kind:        domain.EventDispatchRequested,
		Source:      domain.SourceSystem,
		Payload: map[string]any{
			"work_item_id":    item.ID,
			"capability":      "review.complementary",
			"cultivar":        "review.complementary",
			"origin_token_id": f.root.Token.ID,
			"reason":          "supervisor-fixture",
		},
	}); err != nil {
		t.Fatalf("append demand for %s: %v", title, err)
	}
	if err := tx.Commit(f.ctx); err != nil {
		t.Fatalf("commit demand: %v", err)
	}
	return item
}

func (f *supervisorFixture) assignedEventCount(t *testing.T, itemID uuid.UUID) int {
	t.Helper()
	var n int
	if err := f.pool.QueryRow(f.ctx,
		`SELECT count(*) FROM events WHERE subject_id=$1 AND kind=$2`,
		itemID, domain.EventWorkItemAssigned).Scan(&n); err != nil {
		t.Fatalf("count assigned events: %v", err)
	}
	return n
}

func runOnceOutput(t *testing.T, sup *listenerSupervisor) string {
	t.Helper()
	var buf strings.Builder
	if err := sup.runOnce(context.Background(), &buf); err != nil {
		t.Fatalf("runOnce: %v", err)
	}
	return strings.TrimSpace(buf.String())
}

// TestSupervisorRestartDerivationIntegration: demand appended while the
// supervisor is DOWN is claimed on the next start (no gap), candidates go in
// deterministic (dispatch_event_seq, work_item_id) order, and a restart in
// FOCUSED resumes the held generation without a duplicate assignment.
func TestSupervisorRestartDerivationIntegration(t *testing.T) {
	f := newSupervisorFixture(t, "sup-restart")

	// Both demands exist before any supervisor ran: the mint-before-snapshot
	// pass must see them and take the LOWEST (seq, work_item_id) first.
	first := f.spawnDemand(t, "restart-demand-a")
	second := f.spawnDemand(t, "restart-demand-b")

	out := runOnceOutput(t, f.sup)
	if !strings.Contains(out, "state=focused") || !strings.Contains(out, first.ID.String()) {
		t.Fatalf("first pass = %q, want focused on the first-seq demand %s", out, first.ID)
	}
	if got := f.assignedEventCount(t, first.ID); got != 1 {
		t.Fatalf("assigned events on first = %d, want 1", got)
	}
	if got := f.assignedEventCount(t, second.ID); got != 0 {
		t.Fatalf("assigned events on second = %d, want 0 while focused", got)
	}

	// Restart in FOCUSED: derivation resumes the held generation and never
	// claims new demand while the assignment is active.
	out = runOnceOutput(t, f.sup)
	if !strings.Contains(out, "state=focused") || !strings.Contains(out, first.ID.String()) {
		t.Fatalf("restart pass = %q, want focused on %s", out, first.ID)
	}
	if got := f.assignedEventCount(t, first.ID); got != 1 {
		t.Fatalf("restart minted a duplicate assignment: %d events", got)
	}
	if got := f.assignedEventCount(t, second.ID); got != 0 {
		t.Fatalf("focused restart claimed new demand: %d events on second", got)
	}
}

// TestSupervisorReleaseRestoresLatestBasePolicyIntegration: while FOCUSED
// the admin replaces the base policy; after the holder yields, the next
// derivation runs under the NEW revision (revision-specific cursor) and the
// remaining open demand is claimed — nothing was lost across the focus.
func TestSupervisorReleaseRestoresLatestBasePolicyIntegration(t *testing.T) {
	f := newSupervisorFixture(t, "sup-release")
	first := f.spawnDemand(t, "release-demand-a")
	second := f.spawnDemand(t, "release-demand-b")

	if out := runOnceOutput(t, f.sup); !strings.Contains(out, first.ID.String()) {
		t.Fatalf("first pass = %q, want focused on %s", out, first.ID)
	}
	priorCursor := ""
	entries, err := os.ReadDir(f.sup.cursorDir)
	if err != nil {
		t.Fatalf("read cursor dir: %v", err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "demand-") {
			priorCursor = e.Name()
		}
	}
	if priorCursor == "" {
		t.Fatal("no revision-specific demand cursor was minted")
	}

	// Policy replaced while focused (admin full replacement, new revision).
	reg, err := f.listeners.Get(f.ctx, f.reg.ID)
	if err != nil {
		t.Fatalf("read registration: %v", err)
	}
	if _, err := f.listeners.SetPolicy(f.ctx, f.reg.ID, listeners.SetPolicyInput{
		Policy: listeners.Policy{
			Predicates:               []listeners.PredicateWire{{Kind: "work_item_tree", WorkItemID: f.tree.ID.String()}},
			Capabilities:             []string{"review.complementary"},
			MaxConcurrentAssignments: 1,
		},
		ObservedPolicyEventID: reg.PolicyEventID,
		Actor:                 f.admin.Token,
	}); err != nil {
		t.Fatalf("replace policy while focused: %v", err)
	}

	// The holder yields its exact generation; the release closes the epoch.
	// The finished item is then canceled — a yielded-but-still-open item is
	// DELIBERATELY re-claimable demand, so closing it is what moves the
	// deterministic order onward.
	assignment, err := f.workSvc.GetAssignment(f.ctx, first.ID)
	if err != nil {
		t.Fatalf("read assignment: %v", err)
	}
	if _, err := f.workSvc.Yield(f.ctx, first.ID, assignment.AssignmentEventID, f.principal.Token); err != nil {
		t.Fatalf("yield: %v", err)
	}
	if _, err := f.workSvc.Transition(f.ctx, first.ID, domain.WorkItemCanceled, "supervisor fixture close", f.root.Token); err != nil {
		t.Fatalf("cancel first: %v", err)
	}

	// Next derivation: IDLE under the LATEST policy revision, and the still
	// open demand is claimed — the focus lost nothing.
	out := runOnceOutput(t, f.sup)
	if !strings.Contains(out, "state=focused") || !strings.Contains(out, second.ID.String()) {
		t.Fatalf("post-release pass = %q, want focused on remaining demand %s", out, second.ID)
	}
	updated, err := f.listeners.Get(f.ctx, f.reg.ID)
	if err != nil {
		t.Fatalf("re-read registration: %v", err)
	}
	newCursor := filepath.Join(f.sup.cursorDir, "demand-"+updated.PolicyEventID.String()+".cursor")
	if _, err := os.Stat(newCursor); err != nil {
		t.Fatalf("no cursor for the latest policy revision %s: %v", updated.PolicyEventID, err)
	}
	if filepath.Base(newCursor) == priorCursor {
		t.Fatal("cursor identity did not change with the policy revision")
	}
}

// TestSupervisorStreamClaimAndHandbackIntegration drives the full run()
// loop: demand arriving AFTER the supervisor started wakes the stream and is
// claimed; a yield returns it to IDLE (focus cursor discarded only after the
// observed release) and the next demand is claimed under the restored base
// policy.
func TestSupervisorStreamClaimAndHandbackIntegration(t *testing.T) {
	f := newSupervisorFixture(t, "sup-stream")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- f.sup.run(ctx) }()

	waitFor := func(what string, deadline time.Duration, cond func() bool) {
		t.Helper()
		end := time.Now().Add(deadline)
		for time.Now().Before(end) {
			if cond() {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Fatalf("timed out waiting for %s", what)
	}

	// Let the supervisor reach idle streaming, then append live demand.
	time.Sleep(500 * time.Millisecond)
	first := f.spawnDemand(t, "stream-demand-a")
	waitFor("live demand claimed", 15*time.Second, func() bool {
		return f.assignedEventCount(t, first.ID) == 1
	})

	// Handback: the holder yields; the supervisor observes the release,
	// discards the focus cursor, and claims the next demand.
	assignment, err := f.workSvc.GetAssignment(f.ctx, first.ID)
	if err != nil {
		t.Fatalf("read assignment: %v", err)
	}
	focusCursor := filepath.Join(f.sup.cursorDir, "focus-"+assignment.AssignmentEventID.String()+".cursor")
	if _, err := f.workSvc.Yield(f.ctx, first.ID, assignment.AssignmentEventID, f.principal.Token); err != nil {
		t.Fatalf("yield: %v", err)
	}
	// Close the yielded item so its still-open demand does not deterministically
	// win the next sweep (a yielded open item is re-claimable by design).
	if _, err := f.workSvc.Transition(f.ctx, first.ID, domain.WorkItemCanceled, "supervisor fixture close", f.root.Token); err != nil {
		t.Fatalf("cancel first: %v", err)
	}
	second := f.spawnDemand(t, "stream-demand-b")
	waitFor("post-release demand claimed", 15*time.Second, func() bool {
		return f.assignedEventCount(t, second.ID) == 1
	})
	waitFor("focus cursor discarded after release", 15*time.Second, func() bool {
		_, err := os.Stat(focusCursor)
		return os.IsNotExist(err)
	})
	if got := f.assignedEventCount(t, first.ID); got != 1 {
		t.Fatalf("first demand re-claimed after release: %d assigned events", got)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("supervisor exited with error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("supervisor did not stop on context cancellation")
	}
}
