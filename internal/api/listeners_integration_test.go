package api

// Listener registration REST surface (listener control plane, slice 2):
// lifecycle, separation of duties, stale-policy pure conflicts, the
// self-narrowing rule, and demand resolution against the persisted listening
// contract. The owner-instruction outcome fixtures (pinned fingerprints plus
// matching/nonmatching/chatter outcomes) live in internal/listeners.

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/listeners"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/workitems"
)

type listenerFixture struct {
	ctx       context.Context
	pool      *pgxpool.Pool
	writer    *events.Writer
	server    *Server
	admin     auth.CreateTokenResult
	root      auth.CreateTokenResult
	principal auth.CreateTokenResult
}

func newListenerFixture(t *testing.T) listenerFixture {
	t.Helper()
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	writer := app.NewEventWriter()
	authSvc := auth.NewService(pool, writer)
	root, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "listener-fixture-root", IsRoot: true, Source: domain.SourceHuman})
	if err != nil {
		t.Fatalf("create root: %v", err)
	}
	admin, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name: "listener-admin", Source: domain.SourceHuman,
		Scopes: []string{access.ScopeListenersAdmin},
		Actor:  &root.Token,
	})
	if err != nil {
		t.Fatalf("create admin: %v", err)
	}
	principal, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name: "listener-principal", Source: domain.SourceAgent,
		Scopes: []string{access.ScopeWorkItemsRead, access.ScopeWorkItemsWriteAll},
		Actor:  &root.Token,
	})
	if err != nil {
		t.Fatalf("create principal: %v", err)
	}
	return listenerFixture{
		ctx: ctx, pool: pool, writer: writer, server: New(pool, nil),
		admin: admin, root: root, principal: principal,
	}
}

func decodeListenerResponse(t *testing.T, body []byte) map[string]any {
	t.Helper()
	var payload struct {
		Listener map[string]any `json:"listener"`
	}
	if err := json.Unmarshal(body, &payload); err != nil || payload.Listener == nil {
		t.Fatalf("decode listener response: %v (%s)", err, body)
	}
	return payload.Listener
}

func TestListenerRegistrationLifecycleIntegration(t *testing.T) {
	f := newListenerFixture(t)
	h := f.server.Handler()

	createBody := []byte(`{"name":"codex-review","principal_token_id":"` + f.principal.Token.ID.String() + `","provider":"codex","capabilities":["review.exact_artifact","review.complementary"]}`)

	// Separation of duties: the agent principal cannot register itself, and
	// root is mint/revoke-only even with the admin scope route open.
	rec := doREST(t, h, http.MethodPost, "/v1/listeners", f.principal.Secret, uuid.NewString(), createBody)
	assertRESTStatus(t, rec, http.StatusForbidden)
	rec = doREST(t, h, http.MethodPost, "/v1/listeners", f.root.Secret, uuid.NewString(), createBody)
	assertRESTStatus(t, rec, http.StatusForbidden)

	rec = doREST(t, h, http.MethodPost, "/v1/listeners", f.admin.Secret, uuid.NewString(), createBody)
	assertRESTStatus(t, rec, http.StatusCreated)
	created := decodeListenerResponse(t, rec.Body.Bytes())
	listenerID, _ := created["id"].(string)
	if listenerID == "" || created["name"] != "codex-review" {
		t.Fatalf("create response incomplete: %v", created)
	}

	// Names are addresses: a duplicate is a pure conflict.
	rec = doREST(t, h, http.MethodPost, "/v1/listeners", f.admin.Secret, uuid.NewString(), createBody)
	assertRESTStatus(t, rec, http.StatusConflict)
	assertErrorCode(t, rec, "listener_name_taken")

	// Names are canonically resolvable over REST (MCP's name form mirrors
	// this shape).
	rec = doREST(t, h, http.MethodGet, "/v1/listeners/by-name/codex-review", f.admin.Secret, "", nil)
	assertRESTStatus(t, rec, http.StatusOK)
	if byName := decodeListenerResponse(t, rec.Body.Bytes()); byName["id"] != listenerID {
		t.Fatalf("by-name resolution = %v, want %s", byName["id"], listenerID)
	}
	rec = doREST(t, h, http.MethodGet, "/v1/listeners/by-name/nobody-here", f.admin.Secret, "", nil)
	assertRESTStatus(t, rec, http.StatusNotFound)

	// Admin sets the initial policy (no observed revision yet).
	policyBody := []byte(`{"policy":{"predicates":[],"capabilities":["review.exact_artifact"],"max_concurrent_assignments":1,"focus":"claimed_work_item_tree"}}`)
	rec = doREST(t, h, http.MethodPost, "/v1/listeners/"+listenerID+"/policy", f.admin.Secret, uuid.NewString(), policyBody)
	assertRESTStatus(t, rec, http.StatusOK)
	withPolicy := decodeListenerResponse(t, rec.Body.Bytes())
	policyEventID, _ := withPolicy["policy_event_id"].(string)
	// The all-eligible policy has an empty predicate set and therefore an
	// empty fingerprint — same identity contract as an unfiltered feed lane.
	if policyEventID == "" || withPolicy["policy"] == nil {
		t.Fatalf("policy response incomplete: %v", withPolicy)
	}

	// A replacement that does not name the observed revision is a pure
	// conflict appending nothing (the design's stale-policy rule).
	rec = doREST(t, h, http.MethodPost, "/v1/listeners/"+listenerID+"/policy", f.admin.Secret, uuid.NewString(), policyBody)
	assertRESTStatus(t, rec, http.StatusConflict)
	assertErrorCode(t, rec, "stale_policy_revision")

	// The principal may narrow its own policy (added predicate, same
	// capability subset)...
	narrowBody := []byte(`{"policy":{"predicates":[{"kind":"kind_include","event_kinds":["dispatch.requested"]}],"capabilities":["review.exact_artifact"],"max_concurrent_assignments":1},"observed_policy_event_id":"` + policyEventID + `"}`)
	rec = doREST(t, h, http.MethodPost, "/v1/listeners/"+listenerID+"/policy", f.principal.Secret, uuid.NewString(), narrowBody)
	assertRESTStatus(t, rec, http.StatusOK)
	narrowed := decodeListenerResponse(t, rec.Body.Bytes())
	narrowedEventID, _ := narrowed["policy_event_id"].(string)

	// ...but cannot widen back to the un-lensed policy.
	widenBody := []byte(`{"policy":{"predicates":[],"capabilities":["review.exact_artifact","review.complementary"],"max_concurrent_assignments":1},"observed_policy_event_id":"` + narrowedEventID + `"}`)
	rec = doREST(t, h, http.MethodPost, "/v1/listeners/"+listenerID+"/policy", f.principal.Secret, uuid.NewString(), widenBody)
	assertRESTStatus(t, rec, http.StatusForbidden)
	assertErrorCode(t, rec, "listener_operation_not_authorized")

	// Unknown predicate kinds fail closed.
	badBody := []byte(`{"policy":{"predicates":[{"kind":"vibes"}],"max_concurrent_assignments":1},"observed_policy_event_id":"` + narrowedEventID + `"}`)
	rec = doREST(t, h, http.MethodPost, "/v1/listeners/"+listenerID+"/policy", f.admin.Secret, uuid.NewString(), badBody)
	assertRESTStatus(t, rec, http.StatusBadRequest)

	// So do non-demand projections: this release pins listener policies to
	// the immutable dispatch demand lane.
	badProjection := []byte(`{"policy":{"projection":"activity","predicates":[],"max_concurrent_assignments":1},"observed_policy_event_id":"` + narrowedEventID + `"}`)
	rec = doREST(t, h, http.MethodPost, "/v1/listeners/"+listenerID+"/policy", f.admin.Secret, uuid.NewString(), badProjection)
	assertRESTStatus(t, rec, http.StatusBadRequest)
	assertErrorCode(t, rec, "invalid_listener_request")

	// Credential rotation preserves the stable address.
	authSvc := auth.NewService(f.pool, f.writer)
	rotated, err := authSvc.CreateToken(f.ctx, auth.CreateTokenInput{
		Name: "listener-principal-rotated", Source: domain.SourceAgent,
		Scopes: []string{access.ScopeWorkItemsRead}, Actor: &f.root.Token,
	})
	if err != nil {
		t.Fatalf("create rotated principal: %v", err)
	}
	rec = doREST(t, h, http.MethodPost, "/v1/listeners/"+listenerID+"/credential-bindings", f.admin.Secret, uuid.NewString(),
		[]byte(`{"principal_token_id":"`+rotated.Token.ID.String()+`"}`))
	assertRESTStatus(t, rec, http.StatusOK)
	rebound := decodeListenerResponse(t, rec.Body.Bytes())
	if rebound["principal_token_id"] != rotated.Token.ID.String() || rebound["id"] != listenerID || rebound["name"] != "codex-review" {
		t.Fatalf("rebinding changed the address: %v", rebound)
	}

	// Retirement tombstones; the address then refuses policy changes.
	rec = doREST(t, h, http.MethodPost, "/v1/listeners/"+listenerID+"/retire", f.admin.Secret, uuid.NewString(), []byte(`{"reason":"fixture"}`))
	assertRESTStatus(t, rec, http.StatusOK)
	rec = doREST(t, h, http.MethodPost, "/v1/listeners/"+listenerID+"/policy", f.admin.Secret, uuid.NewString(), widenBody)
	assertRESTStatus(t, rec, http.StatusConflict)
	assertErrorCode(t, rec, "listener_retired")

	// Retired registrations vanish from the default list but stay readable.
	rec = doREST(t, h, http.MethodGet, "/v1/listeners", f.admin.Secret, "", nil)
	assertRESTStatus(t, rec, http.StatusOK)
	if body := rec.Body.String(); len(body) > 0 && body != `{"listeners":[]}`+"\n" && jsonHasListener(t, rec.Body.Bytes(), "codex-review") {
		t.Fatalf("retired listener still in default list: %s", body)
	}
	rec = doREST(t, h, http.MethodGet, "/v1/listeners?include_retired=true", f.admin.Secret, "", nil)
	assertRESTStatus(t, rec, http.StatusOK)
	if !jsonHasListener(t, rec.Body.Bytes(), "codex-review") {
		t.Fatalf("retired listener missing from include_retired list: %s", rec.Body.String())
	}
}

func jsonHasListener(t *testing.T, body []byte, name string) bool {
	t.Helper()
	var payload struct {
		Listeners []map[string]any `json:"listeners"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("decode listeners list: %v (%s)", err, body)
	}
	for _, l := range payload.Listeners {
		if l["name"] == name {
			return true
		}
	}
	return false
}

// TestListenerPolicyIdempotencyDiscriminator pins the transport half of
// LCP2-R2-B1: under the idempotency contract the event discriminator is the
// caller's (token, scope, key) identity, so a RETRY of one logical action
// (same key) replays without a second event, while a REPEATED action with a
// fresh key mints a distinct event even when the event payload is identical
// to an earlier revision (the A -> B -> A cycle).
func TestListenerPolicyIdempotencyDiscriminator(t *testing.T) {
	f := newListenerFixture(t)
	h := f.server.Handler()

	createBody := []byte(`{"name":"cycle-rest","principal_token_id":"` + f.principal.Token.ID.String() + `","provider":"codex","capabilities":["review.exact_artifact","review.complementary"]}`)
	rec := doREST(t, h, http.MethodPost, "/v1/listeners", f.admin.Secret, uuid.NewString(), createBody)
	assertRESTStatus(t, rec, http.StatusCreated)
	listenerID, _ := decodeListenerResponse(t, rec.Body.Bytes())["id"].(string)

	countPolicyEvents := func() int {
		t.Helper()
		var n int
		if err := f.pool.QueryRow(f.ctx,
			`SELECT count(*) FROM events WHERE subject_kind='listener' AND subject_id=$1 AND kind=$2`,
			listenerID, domain.EventListenerPolicySet).Scan(&n); err != nil {
			t.Fatalf("count policy events: %v", err)
		}
		return n
	}
	setPolicy := func(key, body string) string {
		t.Helper()
		rec := doREST(t, h, http.MethodPost, "/v1/listeners/"+listenerID+"/policy", f.admin.Secret, key, []byte(body))
		assertRESTStatus(t, rec, http.StatusOK)
		id, _ := decodeListenerResponse(t, rec.Body.Bytes())["policy_event_id"].(string)
		return id
	}

	policyA := `{"policy":{"predicates":[],"capabilities":["review.exact_artifact"],"max_concurrent_assignments":1}`
	policyB := `{"policy":{"predicates":[],"capabilities":["review.complementary"],"max_concurrent_assignments":1}`
	eventA1 := setPolicy(uuid.NewString(), policyA+`}`)
	eventB := setPolicy(uuid.NewString(), policyB+`,"observed_policy_event_id":"`+eventA1+`"}`)
	// The A -> B -> A cycle: identical event payload to A1, fresh key —
	// must be a THIRD event and the projection must land on A, not stay B.
	finalKey := uuid.NewString()
	finalBody := policyA + `,"observed_policy_event_id":"` + eventB + `"}`
	eventA2 := setPolicy(finalKey, finalBody)
	if eventA2 == eventA1 {
		t.Fatalf("repeated policy payload collapsed onto the first event id %s", eventA1)
	}
	if got := countPolicyEvents(); got != 3 {
		t.Fatalf("policy_set events = %d, want 3", got)
	}

	// Retrying the SAME logical action (same key, same body) replays the
	// recorded response and appends nothing.
	if retried := setPolicy(finalKey, finalBody); retried != eventA2 {
		t.Fatalf("retry returned a different revision: %s != %s", retried, eventA2)
	}
	if got := countPolicyEvents(); got != 3 {
		t.Fatalf("retry appended an event: policy_set = %d, want 3", got)
	}
}

// TestListenerDemandResolutionIntegration is the slice-2 exit: a producer
// presents the id of a DURABLE dispatch demand event — never a bearer UUID
// and never caller-asserted routing fields. Resolution derives capability and
// origin from the event payload, lineage from relations; it is deterministic
// (registration order), capability narrowing reroutes, a policy-less
// registration is not routable, and non-demand events never resolve. (The
// pure outcome fixtures for the four owner instructions live in
// internal/listeners/demand_test.go.)
func TestListenerDemandResolutionIntegration(t *testing.T) {
	f := newListenerFixture(t)
	svc := listeners.NewService(f.pool, f.writer)

	// Demand context: a HUMAN-created tree where Fable (an agent) authored
	// the latest substantive work — codex's LCP2-R2-B2 scenario: the demand
	// must attribute to Fable, not the human creator and not the system
	// author of the dispatch event.
	authSvc := auth.NewService(f.pool, f.writer)
	fable, err := authSvc.CreateToken(f.ctx, auth.CreateTokenInput{Name: "demand-origin-fable", Source: domain.SourceAgent, Actor: &f.root.Token})
	if err != nil {
		t.Fatalf("create fable origin: %v", err)
	}
	workSvc := workitems.NewService(f.pool, f.writer)
	tree, err := workSvc.Create(f.ctx, workitems.CreateInput{Title: "demand-tree", Actor: f.root.Token})
	if err != nil {
		t.Fatalf("create tree: %v", err)
	}
	fableItem, err := workSvc.SpawnChild(f.ctx, tree.ID, workitems.CreateInput{Title: "demand-item", Actor: f.root.Token})
	if err != nil {
		t.Fatalf("spawn demand item: %v", err)
	}
	otherItem, err := workSvc.Create(f.ctx, workitems.CreateInput{Title: "demand-other-tree", Actor: f.root.Token})
	if err != nil {
		t.Fatalf("create other item: %v", err)
	}

	// appendDemand writes the durable dispatch event the way the reconciler
	// does: system-authored, with capability and originating principal in the
	// payload.
	appendDemand := func(itemID uuid.UUID, capability string, origin uuid.UUID, extra map[string]any) uuid.UUID {
		t.Helper()
		payload := map[string]any{
			"work_item_id":           itemID,
			"cultivar":               capability,
			"capability":             capability,
			"reason":                 "listener-demand-fixture",
			"source_reconciler_pass": "dispatch",
		}
		if origin != uuid.Nil {
			payload["origin_token_id"] = origin
		}
		for k, v := range extra {
			payload[k] = v
		}
		tx, err := f.pool.Begin(f.ctx)
		if err != nil {
			t.Fatalf("begin demand tx: %v", err)
		}
		defer func() { _ = tx.Rollback(f.ctx) }()
		id, _, err := f.writer.Append(f.ctx, tx, events.Spec{
			SubjectKind: domain.SubjectWorkItem,
			SubjectID:   itemID,
			Kind:        domain.EventDispatchRequested,
			Source:      domain.SourceSystem,
			Payload:     payload,
		})
		if err != nil {
			t.Fatalf("append demand: %v", err)
		}
		if err := tx.Commit(f.ctx); err != nil {
			t.Fatalf("commit demand: %v", err)
		}
		return id
	}
	fableDemand := appendDemand(fableItem.ID, "review.complementary", fable.Token.ID, nil)
	humanDemand := appendDemand(otherItem.ID, "review.complementary", f.root.Token.ID, nil)

	first, err := svc.Register(f.ctx, listeners.RegisterInput{
		Name: "codex-listener", PrincipalTokenID: f.principal.Token.ID,
		Capabilities: []string{"review.complementary", "implement.go"}, Actor: f.admin.Token,
	})
	if err != nil {
		t.Fatalf("register first: %v", err)
	}
	second, err := svc.Register(f.ctx, listeners.RegisterInput{
		Name: "fable-listener", PrincipalTokenID: f.principal.Token.ID,
		Capabilities: []string{"review.complementary"}, Actor: f.admin.Token,
	})
	if err != nil {
		t.Fatalf("register second: %v", err)
	}

	// LCP2-R2-B3: neither registration has a policy yet, so neither is
	// routable — no broad-routing gap between registration and the intended
	// lens.
	if _, err := svc.ResolveForDemand(f.ctx, fableDemand); err == nil {
		t.Fatal("policy-less registrations resolved demand")
	}

	// Admin establishes both lenses; the first-registered eligible listener
	// wins deterministically.
	broadPolicy := listeners.Policy{Capabilities: []string{"review.complementary"}, MaxConcurrentAssignments: 1}
	if _, err := svc.SetPolicy(f.ctx, first.ID, listeners.SetPolicyInput{Policy: broadPolicy, Actor: f.admin.Token}); err != nil {
		t.Fatalf("set first policy: %v", err)
	}
	if _, err := svc.SetPolicy(f.ctx, second.ID, listeners.SetPolicyInput{Policy: broadPolicy, Actor: f.admin.Token}); err != nil {
		t.Fatalf("set second policy: %v", err)
	}
	got, err := svc.ResolveForDemand(f.ctx, fableDemand)
	if err != nil || got.ID != first.ID {
		t.Fatalf("resolution = %v (%v), want first-registered %s", got.ID, err, first.ID)
	}

	// Narrowing the first listener's policy to implement.go removes it from
	// review routing; resolution deterministically falls to the second.
	reg1, err := svc.Get(f.ctx, first.ID)
	if err != nil {
		t.Fatalf("read first: %v", err)
	}
	if _, err := svc.SetPolicy(f.ctx, first.ID, listeners.SetPolicyInput{
		Policy:                listeners.Policy{Capabilities: []string{"implement.go"}, MaxConcurrentAssignments: 1},
		ObservedPolicyEventID: reg1.PolicyEventID,
		Actor:                 f.admin.Token,
	}); err != nil {
		t.Fatalf("narrow first policy: %v", err)
	}
	got, err = svc.ResolveForDemand(f.ctx, fableDemand)
	if err != nil || got.ID != second.ID {
		t.Fatalf("post-narrowing resolution = %v (%v), want %s", got.ID, err, second.ID)
	}

	// "Listen to Fable": the actor predicate evaluates against the
	// originating principal recorded ON the demand event — Fable authored
	// the work even though a human created the item and the system authored
	// the dispatch. Human-originated demand refuses.
	reg2, err := svc.Get(f.ctx, second.ID)
	if err != nil {
		t.Fatalf("read second: %v", err)
	}
	if _, err := svc.SetPolicy(f.ctx, second.ID, listeners.SetPolicyInput{
		Policy: listeners.Policy{
			Predicates:               []listeners.PredicateWire{{Kind: "actor", TokenIDs: []string{fable.Token.ID.String()}}},
			Capabilities:             []string{"review.complementary"},
			MaxConcurrentAssignments: 1,
		},
		ObservedPolicyEventID: reg2.PolicyEventID,
		Actor:                 f.admin.Token,
	}); err != nil {
		t.Fatalf("set actor policy: %v", err)
	}
	got, err = svc.ResolveForDemand(f.ctx, fableDemand)
	if err != nil || got.ID != second.ID {
		t.Fatalf("fable-authored demand = %v (%v), want %s", got.ID, err, second.ID)
	}
	if _, err := svc.ResolveForDemand(f.ctx, humanDemand); err == nil {
		t.Fatal("human-originated demand resolved through the Fable-only contract")
	}

	// Tree lineage: pinning the contract to the tree still admits demand on
	// the CHILD item (lineage membership), and refuses out-of-tree demand.
	reg2, err = svc.Get(f.ctx, second.ID)
	if err != nil {
		t.Fatalf("re-read second: %v", err)
	}
	if _, err := svc.SetPolicy(f.ctx, second.ID, listeners.SetPolicyInput{
		Policy: listeners.Policy{
			Predicates:               []listeners.PredicateWire{{Kind: "work_item_tree", WorkItemID: tree.ID.String()}},
			Capabilities:             []string{"review.complementary"},
			MaxConcurrentAssignments: 1,
		},
		ObservedPolicyEventID: reg2.PolicyEventID,
		Actor:                 f.admin.Token,
	}); err != nil {
		t.Fatalf("set tree policy: %v", err)
	}
	got, err = svc.ResolveForDemand(f.ctx, fableDemand)
	if err != nil || got.ID != second.ID {
		t.Fatalf("in-tree child demand = %v (%v), want %s", got.ID, err, second.ID)
	}
	if _, err := svc.ResolveForDemand(f.ctx, humanDemand); err == nil {
		t.Fatal("out-of-tree demand resolved through a tree-pinned contract")
	}

	// Fail-closed refusals: no such demand event; an event that is not
	// demand (the listener registration event); a demand whose recorded
	// capability nobody offers; a demand missing its originating principal.
	if _, err := svc.ResolveForDemand(f.ctx, uuid.New()); err == nil {
		t.Fatal("nonexistent demand event resolved")
	}
	if _, err := svc.ResolveForDemand(f.ctx, first.StateEventID); err == nil {
		t.Fatal("non-demand event kind resolved to a listener")
	}
	unofferedDemand := appendDemand(fableItem.ID, "capability.nobody-offers", fable.Token.ID, nil)
	if _, err := svc.ResolveForDemand(f.ctx, unofferedDemand); err == nil {
		t.Fatal("recorded-but-unoffered capability resolved")
	}
	orphanDemand := appendDemand(fableItem.ID, "review.complementary", uuid.Nil, map[string]any{"reason": "no-origin-fixture"})
	if _, err := svc.ResolveForDemand(f.ctx, orphanDemand); err == nil {
		t.Fatal("demand without an originating principal resolved")
	}

	// A malformed durable event cannot redirect evaluation: a payload
	// work_item_id that disagrees with the event's subject refuses instead
	// of evaluating policy against a different tree. (A demand-kind event on
	// a non-work-item subject is already rejected at APPEND time by the
	// jobqueue projector; resolution keeps its own subject-kind check as
	// defense-in-depth.)
	redirected := appendDemand(otherItem.ID, "review.complementary", fable.Token.ID,
		map[string]any{"work_item_id": fableItem.ID})
	if _, err := svc.ResolveForDemand(f.ctx, redirected); err == nil {
		t.Fatal("demand whose payload work_item_id disagrees with its subject resolved")
	}
}
