package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestFocusedTreatsActivationConflictAfterHandbackAsRelease(t *testing.T) {
	workItemID := uuid.New()
	assignmentEventID := uuid.New()
	listenerID := uuid.New()
	holderID := uuid.New()
	activationID := uuid.New()
	activationStateEventID := uuid.New()

	var assignmentReads atomic.Int32
	var ensureCalls atomic.Int32
	var beginCalls atomic.Int32
	var statusCalls atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/work-items/"+workItemID.String()+"/assignment":
			if assignmentReads.Add(1) > 1 {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"assignment": map[string]any{
					"assignment_event_id": assignmentEventID,
					"holder_token_id":     holderID,
				},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/listeners/"+listenerID.String()+"/activations/ensure":
			ensureCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"activation": map[string]any{
					"id":                  activationID,
					"state":               "requested",
					"state_event_id":      activationStateEventID,
					"assignment_event_id": assignmentEventID,
					"work_item_id":        workItemID,
					"binding_generation":  "binding-v1",
				},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/listener-activations/"+activationID.String()+"/begin":
			beginCalls.Add(1)
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"error": map[string]any{
					"code":    "listener_activation_conflict",
					"message": "listeneractivation: no matching active assignment",
				},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/work-items/"+workItemID.String()+"/events":
			statusCalls.Add(1)
			w.WriteHeader(http.StatusCreated)
		default:
			t.Errorf("unexpected listener request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)

	cursorDir := t.TempDir()
	focusCursor := filepath.Join(cursorDir, "focus-"+assignmentEventID.String()+".cursor")
	if err := os.WriteFile(focusCursor, []byte("durable-cursor"), 0o600); err != nil {
		t.Fatalf("write focus cursor: %v", err)
	}
	sup := &listenerSupervisor{
		api:                          server.URL,
		token:                        "listener-token",
		name:                         "codex-review",
		cursorDir:                    cursorDir,
		backoff:                      time.Millisecond,
		logger:                       slog.New(slog.NewTextHandler(io.Discard, nil)),
		http:                         server.Client(),
		activationAdapter:            "/unused",
		activationBindingGeneration:  "binding-v1",
		activationConsumerGeneration: "consumer-v1",
	}
	held := heldAssignment{
		WorkItemID:        workItemID,
		AssignmentEventID: assignmentEventID,
		ListenerID:        &listenerID,
	}

	if err := sup.focused(context.Background(), listenerView{ID: listenerID}, held); err != nil {
		t.Fatalf("focused returned handback race as fatal: %v", err)
	}
	if got := assignmentReads.Load(); got != 2 {
		t.Fatalf("assignment projection reads = %d, want 2", got)
	}
	if got := ensureCalls.Load(); got != 1 {
		t.Fatalf("activation ensure calls = %d, want 1", got)
	}
	if got := beginCalls.Load(); got != 1 {
		t.Fatalf("activation begin calls = %d, want 1", got)
	}
	if got := statusCalls.Load(); got != 1 {
		t.Fatalf("release status calls = %d, want 1", got)
	}
	if _, err := os.Stat(focusCursor); !os.IsNotExist(err) {
		t.Fatalf("focus cursor still exists after observed release: %v", err)
	}
}
