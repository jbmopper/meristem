package mcp

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/backlog"
	"github.com/jbmopper/meristem/internal/domain"
)

// TestProviderSafeProfilesHaveRegisteredRenderers is the structural guard that
// makes the enforcement fail-closed by construction: every tool a provider HTTP
// surface exposes must have a provider-safe rendering contract. On the old
// opt-in design a tool could be added to a provider profile allowlist while its
// handler forgot the reduction branch and leaked the raw operator DTO; here that
// omission is a compile-independent test failure instead of a silent leak.
func TestProviderSafeProfilesHaveRegisteredRenderers(t *testing.T) {
	surfaces := map[string]map[string]bool{
		"ProviderSafeReadHTTPProfile": ProviderSafeReadHTTPProfile().allowedTools,
		"ProviderTrackerHTTPProfile":  ProviderTrackerHTTPProfile().allowedTools,
		"ReadOnlyHTTPTools":           ReadOnlyHTTPTools(),
	}
	for surface, allowlist := range surfaces {
		if len(allowlist) == 0 {
			t.Fatalf("%s exposed no tools; the guard would vacuously pass", surface)
		}
		for tool := range allowlist {
			if !providerSafeRenderRegistered(tool) {
				t.Errorf("%s exposes %q with no registered provider-safe renderer; add an entry to providerSafeRenderers", surface, tool)
			}
		}
	}
}

// TestProviderSafeUnregisteredToolFailsClosed proves the fail-closed gate at the
// HTTP boundary: a tool that is on a provider profile's allowlist but has no
// registered renderer is rejected before dispatch with a deterministic error,
// never falling through to a raw handler DTO.
func TestProviderSafeUnregisteredToolFailsClosed(t *testing.T) {
	s := New(Deps{}, ServerInfo{Name: "meristem-test", Version: "test"}, nil)
	// A real tool (registry.list) that is deliberately never provider-safe, plus
	// a wholly synthetic name, both smuggled onto a profile allowlist. Neither
	// has a renderer, so both must be rejected at the boundary.
	for _, tool := range []string{"registry.list", "synthetic.unregistered"} {
		t.Run(tool, func(t *testing.T) {
			profile := &HTTPToolProfile{name: "synthetic-leak", restrictTools: true, allowedTools: toolSet(tool), providerSafeResponses: true}

			// Direct boundary check.
			rerr := s.checkHTTPToolAllowed(json.RawMessage(`{"name":"`+tool+`","arguments":{}}`), HTTPOptions{Profile: profile})
			if rerr == nil {
				t.Fatalf("allowlisted-but-unregistered tool %q was not rejected before dispatch", tool)
			}
			if rerr.Code != errCodeMethodNotFound {
				t.Errorf("rejection code = %d, want %d", rerr.Code, errCodeMethodNotFound)
			}
			if !strings.Contains(rerr.Message, "provider-safe response rendering not registered") {
				t.Errorf("rejection message = %q, want the fail-closed rendering-gate message", rerr.Message)
			}

			// The complete HTTP request path must also fail closed. Exact actor/profile
			// matching may reject this synthetic route before the renderer gate; either
			// way no result body may escape.
			actor := domain.Token{ID: uuid.New(), Source: domain.SourceAgent}
			request := []byte(`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"` + tool + `","arguments":{}}}`)
			resp := s.HandleHTTPMessageWithOptions(context.Background(), request, actor, HTTPOptions{Profile: profile})
			var envelope rpcMessage
			if err := json.Unmarshal(resp.Body, &envelope); err != nil {
				t.Fatalf("decode full-path response: %v body=%s", err, resp.Body)
			}
			if envelope.Error == nil {
				t.Fatalf("full request path returned a result for allowlisted-but-unregistered tool %q: %s", tool, resp.Body)
			}
			if len(envelope.Result) != 0 {
				t.Fatalf("full request path leaked a result alongside its error for %q: %s", tool, resp.Body)
			}
		})
	}
}

// TestRenderProviderSafeResultFailsClosed exercises the post-handler renderer
// dispatcher directly. Both structural failure modes must return an error rather
// than emit an ordinary shape: an unregistered tool, and a registered tool whose
// handler returned an untagged carrier.
func TestRenderProviderSafeResultFailsClosed(t *testing.T) {
	t.Run("unregistered tool", func(t *testing.T) {
		_, rerr := renderProviderSafeResult("registry.list", map[string]any{"anything": true})
		if rerr == nil {
			t.Fatal("unregistered tool rendered instead of failing closed")
		}
		if rerr.Code != errCodeMethodNotFound {
			t.Errorf("code = %d, want %d", rerr.Code, errCodeMethodNotFound)
		}
	})

	t.Run("carrier type mismatch", func(t *testing.T) {
		// work_items.get is registered, but handing its renderer the ordinary DTO
		// map (as a handler that forgot to tag its result would) must fail closed.
		_, rerr := renderProviderSafeResult("work_items.get", map[string]any{
			"work_item": map[string]any{"created_by": "leak", "state_reason": "leak"},
		})
		if rerr == nil {
			t.Fatal("carrier mismatch rendered instead of failing closed")
		}
		if rerr.Code != errCodeInternal {
			t.Errorf("code = %d, want %d", rerr.Code, errCodeInternal)
		}
		if strings.Contains(rerr.Message, "leak") {
			t.Errorf("mismatch error echoed the unrendered payload: %s", rerr.Message)
		}
	})
}

// TestProviderSafeAppendEventRendererIsTypedAndExact pins the acknowledgement
// to a typed carrier and a two-field wire shape. A raw map must fail closed so
// future fields cannot leak through an apparent "safe acknowledgement".
func TestProviderSafeAppendEventRendererIsTypedAndExact(t *testing.T) {
	const tool = "work_items.append_event"
	if !providerSafeRenderRegistered(tool) {
		t.Fatalf("%s must be registered to clear the fail-closed boundary gate", tool)
	}
	id := uuid.New()
	ack := providerSafeAppendEventResult{workItemID: id, appended: true}
	rendered, rerr := renderProviderSafeResult(tool, ack)
	if rerr != nil {
		t.Fatalf("append-event render failed: %+v", rerr)
	}
	got, ok := rendered.(map[string]any)
	if !ok {
		t.Fatalf("append-event renderer returned %T", rendered)
	}
	if len(got) != 2 || got["appended"] != true || got["work_item_id"] != id {
		t.Fatalf("acknowledgement was altered: %+v", got)
	}
	if _, rerr := renderProviderSafeResult(tool, map[string]any{
		"work_item_id": id,
		"appended":     true,
		"future_field": "must-not-pass-through",
	}); rerr == nil {
		t.Fatal("raw append-event map rendered instead of failing closed")
	}
}

func TestProviderSafeReadinessRendererProjectsEveryGroup(t *testing.T) {
	privateReason := "PRIVATE-READINESS-STATE-REASON"
	now := time.Date(2026, 7, 16, 20, 0, 0, 0, time.UTC)
	item := func(title string) backlog.Item {
		return backlog.Item{
			ID:                         uuid.New(),
			Title:                      title,
			State:                      domain.WorkItemBlocked,
			StateReason:                &privateReason,
			HumanReviewStatus:          domain.HumanReviewBlocked,
			SuggestedConvergenceChecks: []string{"event:reviewed"},
			StateEnteredAt:             now,
			UpdatedAt:                  now,
			Tags:                       []string{"blocked"},
		}
	}
	summary := backlog.Summary{
		Contract:    backlog.Contract,
		Source:      "test projection",
		Limit:       0,
		AsOf:        now,
		Totals:      backlog.Totals{Visible: 5, NonTerminal: 5},
		StateCounts: map[domain.WorkItemState]int{domain.WorkItemBlocked: 5},
		Groups: backlog.Groups{
			V1Substrate: []backlog.Item{item("V1-GROUP-CANARY")},
			ReadyNext:   []backlog.Item{item("READY-GROUP-CANARY")},
			Blockers:    []backlog.Item{item("BLOCKER-GROUP-CANARY")},
			Running:     []backlog.Item{item("RUNNING-GROUP-CANARY")},
			StaleNoise:  []backlog.Item{item("STALE-GROUP-CANARY")},
		},
		SpecSeedDrift:       []string{"missing_refresh_item:R9"},
		ClassificationRules: []string{"explicit test rule"},
	}

	rendered, rerr := renderProviderSafeResult("backlog.readiness", providerSafeReadinessResult{summary: summary})
	if rerr != nil {
		t.Fatalf("readiness render failed: %+v", rerr)
	}
	encoded, err := json.Marshal(rendered)
	if err != nil {
		t.Fatal(err)
	}
	text := string(encoded)
	for _, required := range []string{
		"v1_substrate", "ready_next", "blockers", "running", "stale_noise",
		"V1-GROUP-CANARY", "READY-GROUP-CANARY", "BLOCKER-GROUP-CANARY",
		"RUNNING-GROUP-CANARY", "STALE-GROUP-CANARY", "explicit test rule",
	} {
		if !strings.Contains(text, required) {
			t.Errorf("provider readiness omitted %q: %s", required, text)
		}
	}
	for _, forbidden := range []string{"state_reason", privateReason} {
		if strings.Contains(text, forbidden) {
			t.Errorf("provider readiness leaked %q: %s", forbidden, text)
		}
	}
}
