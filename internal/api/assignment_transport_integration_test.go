package api

// Canonical assignment transports (listener control plane, slice 1): REST
// claim/get-assignment/yield over the existing internal service. The service
// semantics — atomic claim ledger, same-holder idempotency, no takeover,
// opportunistic expiry — are pinned by internal/workitems tests; these tests
// pin the transport contract: status codes, conflict payloads, pure-refusal
// key preservation, and event visibility through the API surface.

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/workitems"
)

type assignmentTransportFixture struct {
	ctx    context.Context
	pool   *pgxpool.Pool
	writer *events.Writer
	work   *workitems.Service
	server *Server
	root   auth.CreateTokenResult
	tree   domain.WorkItem
}

func newAssignmentTransportFixture(t *testing.T) assignmentTransportFixture {
	t.Helper()
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	writer := app.NewEventWriter()
	authSvc := auth.NewService(pool, writer)
	root, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "assignment-transport-root", IsRoot: true, Source: domain.SourceHuman})
	if err != nil {
		t.Fatalf("create root: %v", err)
	}
	work := workitems.NewService(pool, writer)
	tree, err := work.Create(ctx, workitems.CreateInput{Title: "assignment-transport-tree", Actor: root.Token})
	if err != nil {
		t.Fatalf("create tree: %v", err)
	}
	return assignmentTransportFixture{
		ctx: ctx, pool: pool, writer: writer, work: work,
		server: New(pool, nil), root: root, tree: tree,
	}
}

func (f assignmentTransportFixture) spawnClaimable(t *testing.T, title string) domain.WorkItem {
	t.Helper()
	item, err := f.work.SpawnChild(f.ctx, f.tree.ID, workitems.CreateInput{
		Title: title, State: domain.WorkItemRunning,
		SuggestedConvergenceChecks: []string{"event:assignment_transport"},
		HumanReviewStatus:          domain.HumanReviewWavedThrough,
		Actor:                      f.root.Token,
	})
	if err != nil {
		t.Fatalf("spawn %s: %v", title, err)
	}
	return item
}

func (f assignmentTransportFixture) scopedAgent(t *testing.T, name string) auth.CreateTokenResult {
	t.Helper()
	authSvc := auth.NewService(f.pool, f.writer)
	result, err := authSvc.CreateToken(f.ctx, auth.CreateTokenInput{
		Name:   name,
		Source: domain.SourceAgent,
		Scopes: []string{
			access.ScopeWorkItemsRead,
			access.ScopeWorkItemsWrite,
			access.ScopeFeedReadAssigned,
			"work_items.tree:" + f.tree.ID.String(),
		},
		Actor: &f.root.Token,
	})
	if err != nil {
		t.Fatalf("create scoped token %s: %v", name, err)
	}
	return result
}

func decodeAssignmentResponse(t *testing.T, body []byte) map[string]any {
	t.Helper()
	var payload struct {
		Assignment map[string]any `json:"assignment"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("decode assignment response: %v (%s)", err, body)
	}
	if payload.Assignment == nil {
		t.Fatalf("response carried no assignment object: %s", body)
	}
	return payload.Assignment
}

func TestAssignmentTransportLifecycleIntegration(t *testing.T) {
	f := newAssignmentTransportFixture(t)
	item := f.spawnClaimable(t, "lifecycle")
	holderA := f.scopedAgent(t, "assignment-holder-a")
	holderB := f.scopedAgent(t, "assignment-holder-b")
	base := "/v1/work-items/" + item.ID.String()

	rec := doREST(t, f.server.Handler(), http.MethodPost, base+"/claim", holderA.Secret, uuid.NewString(), nil)
	assertRESTStatus(t, rec, http.StatusOK)
	claimed := decodeAssignmentResponse(t, rec.Body.Bytes())
	if claimed["holder_token_id"] != holderA.Token.ID.String() {
		t.Fatalf("claim holder = %v, want %s", claimed["holder_token_id"], holderA.Token.ID)
	}
	assignmentEventID, _ := claimed["assignment_event_id"].(string)
	if assignmentEventID == "" {
		t.Fatalf("claim response missing assignment_event_id: %v", claimed)
	}

	// Same-holder retry under a fresh key is the service-level idempotent
	// success: same generation, no second assigned event.
	rec = doREST(t, f.server.Handler(), http.MethodPost, base+"/claim", holderA.Secret, uuid.NewString(), nil)
	assertRESTStatus(t, rec, http.StatusOK)
	if again := decodeAssignmentResponse(t, rec.Body.Bytes()); again["assignment_event_id"] != assignmentEventID {
		t.Fatalf("same-holder retry changed generation: %v -> %v", assignmentEventID, again["assignment_event_id"])
	}

	// Competing claim: typed conflict carrying holder identity, and — because
	// the refusal is pure — the loser's idempotency key stays usable.
	loserKey := uuid.NewString()
	rec = doREST(t, f.server.Handler(), http.MethodPost, base+"/claim", holderB.Secret, loserKey, nil)
	assertRESTStatus(t, rec, http.StatusConflict)
	var conflict struct {
		Error struct {
			Code              string `json:"code"`
			HolderTokenID     string `json:"holder_token_id"`
			AssignmentEventID string `json:"assignment_event_id"`
			ExpiresAt         string `json:"expires_at"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &conflict); err != nil {
		t.Fatalf("decode conflict: %v (%s)", err, rec.Body.String())
	}
	if conflict.Error.Code != "claim_held" || conflict.Error.HolderTokenID != holderA.Token.ID.String() ||
		conflict.Error.AssignmentEventID != assignmentEventID || conflict.Error.ExpiresAt == "" {
		t.Fatalf("claim_held payload incomplete: %+v", conflict.Error)
	}

	rec = doREST(t, f.server.Handler(), http.MethodGet, base+"/assignment", holderB.Secret, "", nil)
	assertRESTStatus(t, rec, http.StatusOK)
	if got := decodeAssignmentResponse(t, rec.Body.Bytes()); got["assignment_event_id"] != assignmentEventID {
		t.Fatalf("get assignment = %v, want generation %s", got, assignmentEventID)
	}

	rec = doREST(t, f.server.Handler(), http.MethodPost, base+"/yield", holderB.Secret, uuid.NewString(), nil)
	assertRESTStatus(t, rec, http.StatusConflict)
	assertErrorCode(t, rec, "assignment_not_held")

	rec = doREST(t, f.server.Handler(), http.MethodPost, base+"/yield", holderA.Secret, uuid.NewString(), nil)
	assertRESTStatus(t, rec, http.StatusOK)
	if released := decodeAssignmentResponse(t, rec.Body.Bytes()); released["assignment_event_id"] != assignmentEventID {
		t.Fatalf("yield released %v, want generation %s", released["assignment_event_id"], assignmentEventID)
	}

	rec = doREST(t, f.server.Handler(), http.MethodGet, base+"/assignment", holderA.Secret, "", nil)
	assertRESTStatus(t, rec, http.StatusNotFound)
	assertErrorCode(t, rec, "assignment_not_found")

	// The loser's pure-refusal key was never consumed: the SAME key now claims
	// the released item successfully instead of replaying the 409.
	rec = doREST(t, f.server.Handler(), http.MethodPost, base+"/claim", holderB.Secret, loserKey, nil)
	assertRESTStatus(t, rec, http.StatusOK)
	if won := decodeAssignmentResponse(t, rec.Body.Bytes()); won["holder_token_id"] != holderB.Token.ID.String() {
		t.Fatalf("post-release claim holder = %v, want %s", won["holder_token_id"], holderB.Token.ID)
	}

	// Assignment lifecycle stays audit-only on the default human feed, but
	// the holder's assigned/addressed runtime lane is the narrow exception
	// that wakes a claimant on assign and release: both kinds must be
	// visible there through the transport surface.
	feedRec := doREST(t, f.server.Handler(), http.MethodGet, "/v1/feed?limit=200", holderA.Secret, "", nil)
	assertRESTStatus(t, feedRec, http.StatusOK)
	for _, kind := range []string{"work_item.assigned", "work_item.assignment_released"} {
		if !strings.Contains(feedRec.Body.String(), kind) {
			t.Errorf("assigned lane omitted %s: %s", kind, feedRec.Body.String())
		}
	}
	rootFeed := doREST(t, f.server.Handler(), http.MethodGet, "/v1/feed?limit=200", f.root.Secret, "", nil)
	assertRESTStatus(t, rootFeed, http.StatusOK)
	if strings.Contains(rootFeed.Body.String(), "work_item.assigned") {
		t.Errorf("default feed narrative unexpectedly includes assignment lifecycle: %s", rootFeed.Body.String())
	}
}

func TestAssignmentTransportRaceIntegration(t *testing.T) {
	f := newAssignmentTransportFixture(t)
	item := f.spawnClaimable(t, "race")
	racerA := f.scopedAgent(t, "assignment-racer-a")
	racerB := f.scopedAgent(t, "assignment-racer-b")
	base := "/v1/work-items/" + item.ID.String()

	codes := make([]int, 2)
	var wg sync.WaitGroup
	for i, secret := range []string{racerA.Secret, racerB.Secret} {
		wg.Add(1)
		go func(slot int, bearer string) {
			defer wg.Done()
			rec := doREST(t, f.server.Handler(), http.MethodPost, base+"/claim", bearer, uuid.NewString(), nil)
			codes[slot] = rec.Code
		}(i, secret)
	}
	wg.Wait()

	winners, losers := 0, 0
	for _, code := range codes {
		switch code {
		case http.StatusOK:
			winners++
		case http.StatusConflict:
			losers++
		default:
			t.Fatalf("unexpected race status codes: %v", codes)
		}
	}
	if winners != 1 || losers != 1 {
		t.Fatalf("race produced %d winners and %d conflicts, want exactly 1 and 1", winners, losers)
	}
	var assignedEvents int
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM events WHERE subject_id=$1 AND kind=$2`,
		item.ID, domain.EventWorkItemAssigned).Scan(&assignedEvents); err != nil {
		t.Fatalf("count assigned events: %v", err)
	}
	if assignedEvents != 1 {
		t.Fatalf("race appended %d work_item.assigned events, want exactly 1", assignedEvents)
	}
}

func TestAssignmentTransportExpiredClaimIntegration(t *testing.T) {
	f := newAssignmentTransportFixture(t)
	item := f.spawnClaimable(t, "expired")
	stale := f.scopedAgent(t, "assignment-stale-holder")
	fresh := f.scopedAgent(t, "assignment-fresh-holder")
	base := "/v1/work-items/" + item.ID.String()

	// Seed a one-second lease exactly as the projector expects it, then let
	// it lapse: transport claim by a new holder must release the incumbent as
	// expired and win in the same transaction.
	tx, err := f.pool.BeginTx(f.ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var claimedAt time.Time
	if err := tx.QueryRow(f.ctx, `SELECT clock_timestamp()`).Scan(&claimedAt); err != nil {
		_ = tx.Rollback(f.ctx)
		t.Fatalf("read clock: %v", err)
	}
	if _, _, err := f.writer.Append(f.ctx, tx, events.Spec{
		SubjectKind: domain.SubjectWorkItem, SubjectID: item.ID,
		Kind: domain.EventWorkItemAssigned, Source: domain.SourceAgent, ActorTokenID: &stale.Token.ID,
		Discriminator: "assignment-transport-expired-test",
		Payload: map[string]any{
			"payload_version": 1, "assignee_token_id": stale.Token.ID,
			"mode": domain.WorkItemAssignmentClaim, "lease_seconds": 1,
			"lease_source": "test:one-second",
			"claimed_at":   claimedAt, "expires_at": claimedAt.Add(time.Second),
		},
	}); err != nil {
		_ = tx.Rollback(f.ctx)
		t.Fatalf("append short lease: %v", err)
	}
	if err := tx.Commit(f.ctx); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1100 * time.Millisecond)

	rec := doREST(t, f.server.Handler(), http.MethodPost, base+"/claim", fresh.Secret, uuid.NewString(), nil)
	assertRESTStatus(t, rec, http.StatusOK)
	if got := decodeAssignmentResponse(t, rec.Body.Bytes()); got["holder_token_id"] != fresh.Token.ID.String() {
		t.Fatalf("expired-claim winner = %v, want %s", got["holder_token_id"], fresh.Token.ID)
	}
	var expiredReleases int
	if err := f.pool.QueryRow(f.ctx, `
		SELECT count(*) FROM events
		WHERE subject_id=$1 AND kind=$2 AND payload->>'reason'='expired'`,
		item.ID, domain.EventWorkItemAssignmentReleased).Scan(&expiredReleases); err != nil {
		t.Fatalf("count expired releases: %v", err)
	}
	if expiredReleases != 1 {
		t.Fatalf("expired incumbent releases = %d, want exactly 1", expiredReleases)
	}
}

// TestMisaddressedAssignedLaneContract is the slice-0 contract test for the
// three-token misaddressing failure the listener registration exists to
// remove: an event addressed to one credential of a logical actor is — by
// the shipped lane's own correct rules — invisible to that actor's OTHER
// credentials. Routing by stable listener registration (slice 2) is the fix;
// this test pins the failure mode it fixes.
func TestMisaddressedAssignedLaneContract(t *testing.T) {
	f := newAssignmentTransportFixture(t)
	item := f.spawnClaimable(t, "misaddressed")

	legacy := f.scopedAgentWithFeed(t, "principal-legacy-token")
	interactive := f.scopedAgentWithFeed(t, "principal-interactive-token")
	bridge := f.scopedAgentWithFeed(t, "principal-bridge-token")

	if err := f.work.AppendEvent(f.ctx, item.ID, "review.requested_probe", map[string]any{
		"marker":             "misaddressed-probe",
		"addressee_token_id": legacy.Token.ID,
	}, f.root.Token); err != nil {
		t.Fatalf("append addressed probe: %v", err)
	}

	for _, tc := range []struct {
		name    string
		bearer  string
		visible bool
	}{
		{"addressee sees it", legacy.Secret, true},
		{"interactive credential filtered", interactive.Secret, false},
		{"bridge credential filtered", bridge.Secret, false},
	} {
		rec := doREST(t, f.server.Handler(), http.MethodGet, "/v1/feed?limit=100", tc.bearer, "", nil)
		assertRESTStatus(t, rec, http.StatusOK)
		got := strings.Contains(rec.Body.String(), "misaddressed-probe")
		if got != tc.visible {
			t.Errorf("%s: probe visibility = %v, want %v (%s)", tc.name, got, tc.visible, rec.Body.String())
		}
	}
}

// scopedAgentWithFeed grants the assigned-lane feed scope with tree access.
// Tree scope governs authorization, not lane membership: an in-tree event
// addressed to a DIFFERENT credential stays invisible on this lane, which is
// exactly the property the misaddressing contract pins.
func (f assignmentTransportFixture) scopedAgentWithFeed(t *testing.T, name string) auth.CreateTokenResult {
	t.Helper()
	authSvc := auth.NewService(f.pool, f.writer)
	result, err := authSvc.CreateToken(f.ctx, auth.CreateTokenInput{
		Name:   name,
		Source: domain.SourceAgent,
		Scopes: []string{
			access.ScopeWorkItemsRead,
			access.ScopeFeedReadAssigned,
			"work_items.tree:" + f.tree.ID.String(),
		},
		Actor: &f.root.Token,
	})
	if err != nil {
		t.Fatalf("create feed token %s: %v", name, err)
	}
	return result
}
