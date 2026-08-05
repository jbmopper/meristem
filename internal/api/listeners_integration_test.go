package api

// Listener registration REST surface (listener control plane, slice 2):
// lifecycle, separation of duties, stale-policy pure conflicts, the
// self-narrowing rule, capability resolution, and the four owner-instruction
// policy fixtures with pinned predicate fingerprints.

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

// TestListenerCapabilityResolutionIntegration is the slice-2 exit: a
// producer names a capability, never a bearer UUID, and resolution is
// deterministic (registration order) with policy-narrowed capability sets
// respected.
func TestListenerCapabilityResolutionIntegration(t *testing.T) {
	f := newListenerFixture(t)
	svc := listeners.NewService(f.pool, f.writer)

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

	got, err := svc.ResolveForCapability(f.ctx, "review.complementary")
	if err != nil || got.ID != first.ID {
		t.Fatalf("resolution = %v (%v), want first-registered %s", got.ID, err, first.ID)
	}

	// Narrowing the first listener's policy to implement.go removes it from
	// review routing; resolution deterministically falls to the second.
	if _, err := svc.SetPolicy(f.ctx, first.ID, listeners.SetPolicyInput{
		Policy: listeners.Policy{Capabilities: []string{"implement.go"}, MaxConcurrentAssignments: 1},
		Actor:  f.admin.Token, ActorIsAdminSurface: true,
	}); err != nil {
		t.Fatalf("narrow first policy: %v", err)
	}
	got, err = svc.ResolveForCapability(f.ctx, "review.complementary")
	if err != nil || got.ID != second.ID {
		t.Fatalf("post-narrowing resolution = %v (%v), want %s", got.ID, err, second.ID)
	}

	if _, err := svc.ResolveForCapability(f.ctx, "capability.nobody-offers"); err == nil {
		t.Fatal("resolution of unoffered capability should refuse")
	}
}

// TestOwnerInstructionPolicyFixtures pins the deterministic translation of
// the design's four owner-visible instructions into normalized policies with
// stable predicate fingerprints. Fixed UUIDs keep the fingerprints constant:
// if normalization or the fingerprint contract drifts, these constants break
// loudly — exactly like the feed cursor fingerprint pins.
func TestOwnerInstructionPolicyFixtures(t *testing.T) {
	listenerID := uuid.MustParse("11111111-1111-4111-8111-111111111111")
	fableToken := uuid.MustParse("22222222-2222-4222-8222-222222222222")
	networkingTree := uuid.MustParse("33333333-3333-4333-8333-333333333333")
	registered := []string{"review.complementary"}

	fixtures := []struct {
		instruction string
		policy      listeners.Policy
		fingerprint string
	}{
		{
			instruction: "Listen for everything",
			policy:      listeners.Policy{Predicates: nil, MaxConcurrentAssignments: 1},
			fingerprint: "", // empty predicate set = plain lane, no fingerprint
		},
		{
			instruction: "Listen to Fable",
			policy: listeners.Policy{
				Predicates:               []listeners.PredicateWire{{Kind: "actor", TokenIDs: []string{fableToken.String()}}},
				MaxConcurrentAssignments: 1,
			},
			fingerprint: "d18d5b4adda6101700dcf8f3fdead507",
		},
		{
			instruction: "Listen for networking work",
			policy: listeners.Policy{
				Predicates: []listeners.PredicateWire{
					{Kind: "work_item_tree", WorkItemID: networkingTree.String()},
					{Kind: "kind_include", EventKinds: []string{"dispatch.requested"}},
				},
				MaxConcurrentAssignments: 1,
			},
			fingerprint: "2a7409bb3fdfeecba9cf9608c0c4a977",
		},
		{
			instruction: "Pick up one thing, finish it, then listen again",
			policy:      listeners.Policy{Predicates: nil, MaxConcurrentAssignments: 1, Focus: listeners.FocusClaimedWorkItemTree},
			fingerprint: "",
		},
	}
	for _, fixture := range fixtures {
		normalized, fingerprint, err := listeners.NormalizePolicy(fixture.policy, listenerID, registered)
		if err != nil {
			t.Errorf("%s: normalize: %v", fixture.instruction, err)
			continue
		}
		if normalized.MaxConcurrentAssignments != 1 || normalized.Focus == "" {
			t.Errorf("%s: normalization incomplete: %+v", fixture.instruction, normalized)
		}
		if fixture.fingerprint == "PIN_ME" {
			t.Logf("%s fingerprint: %s", fixture.instruction, fingerprint)
			continue
		}
		if fingerprint != fixture.fingerprint {
			t.Errorf("%s: fingerprint = %q, want pinned %q — predicate identity drifted", fixture.instruction, fingerprint, fixture.fingerprint)
		}
	}
}
