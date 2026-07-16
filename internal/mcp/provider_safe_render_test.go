package mcp

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/uuid"

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
	actor := domain.Token{ID: uuid.New(), Source: domain.SourceHuman}

	// A real tool (registry.list) that is deliberately never provider-safe, plus
	// a wholly synthetic name, both smuggled onto a profile allowlist. Neither
	// has a renderer, so both must be rejected at the boundary.
	for _, tool := range []string{"registry.list", "synthetic.unregistered"} {
		t.Run(tool, func(t *testing.T) {
			profile := &HTTPToolProfile{name: "synthetic-leak", allowedTools: toolSet(tool)}

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

			// Full request path: the handler must never run, so the reply is a
			// JSON-RPC error with no result body.
			resp := s.HandleHTTPMessageWithOptions(
				context.Background(),
				[]byte(`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"`+tool+`","arguments":{}}}`),
				actor,
				HTTPOptions{Profile: profile},
			)
			var envelope struct {
				Result json.RawMessage `json:"result"`
				Error  *rpcError       `json:"error"`
			}
			if err := json.Unmarshal(resp.Body, &envelope); err != nil {
				t.Fatalf("decode response: %v body=%s", err, resp.Body)
			}
			if envelope.Error == nil {
				t.Fatalf("expected a JSON-RPC error, got result: %s", resp.Body)
			}
			if len(envelope.Result) != 0 {
				t.Fatalf("fail-closed reply carried a result body: %s", resp.Body)
			}
			if !strings.Contains(envelope.Error.Message, "provider-safe response rendering not registered") {
				t.Fatalf("unexpected error message: %s", resp.Body)
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

// TestProviderSafeNoReductionEntryPassesThrough documents that the one tool with
// a no-reduction justification (work_items.append_event) is registered (so it
// clears the fail-closed gate) and is emitted unchanged, since its
// acknowledgement joins no DTO and echoes no free-form field.
func TestProviderSafeNoReductionEntryPassesThrough(t *testing.T) {
	const tool = "work_items.append_event"
	if !providerSafeRenderRegistered(tool) {
		t.Fatalf("%s must be registered to clear the fail-closed boundary gate", tool)
	}
	renderer, ok := providerSafeRendererFor(tool)
	if !ok {
		t.Fatalf("%s renderer missing", tool)
	}
	if renderer.providerSafeRenderable() {
		t.Fatalf("%s should be a documented no-reduction pass-through, not a reducer", tool)
	}
	ack := map[string]any{"work_item_id": uuid.New(), "appended": true}
	rendered, rerr := renderProviderSafeResult(tool, ack)
	if rerr != nil {
		t.Fatalf("no-reduction render failed: %+v", rerr)
	}
	got, ok := rendered.(map[string]any)
	if !ok {
		t.Fatalf("no-reduction render changed the type: %T", rendered)
	}
	if got["appended"] != true || got["work_item_id"] != ack["work_item_id"] {
		t.Fatalf("acknowledgement was altered: %+v", got)
	}
}
