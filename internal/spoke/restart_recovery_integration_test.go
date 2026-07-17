package spoke_test

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/api"
	"github.com/jbmopper/meristem/internal/crossnode"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/spoke"
)

// TestSpokeRestartBetweenFetchAndAckResumes pins the restart-recovery cell of
// the fault matrix with a literal process restart, not just an ack-failure
// retry on the same poller (work item 17ce2faf, audit finding G4a).
//
// Sequence: the spoke drains a command and executes it locally — the local
// mutation lands — but the hub refuses the acknowledgement, exactly the state
// a crash between fetch and ack leaves behind. The first poller is then
// DISCARDED, never ticked again, and a brand-new Poller (fresh construction,
// zero shared in-memory state) takes over. The new poller must re-fetch the
// still-pending command, replay the local execution through the original
// idempotency key without a second mutation, and complete the ack — proving
// that nothing load-bearing for resume lives in poller memory.
func TestSpokeRestartBetweenFetchAndAckResumes(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	hubPool, hubToken := newAcceptanceNode(t, ctx, "hub", logger, []string{
		crossnode.QueueWriteScope("spoke-r", crossnode.OperationClassWorkItemsWrite),
		crossnode.OriginScope("hub"),
		crossnode.QueueDrainScope("spoke-r"),
		crossnode.QueueAckScope("spoke-r"),
		access.ScopeFeedRead,
	})
	localPool, localToken := newAcceptanceNode(t, ctx, "spoke-r", logger, []string{
		access.ScopeWorkItemsCreate,
		crossnode.TargetExecuteScope("spoke-r"),
		crossnode.OriginScope("hub"),
	})

	t.Setenv(api.EnvNodeID, "hub")
	hubHandler := api.New(hubPool, logger).Handler()
	// Refuse every ack until the "restart" happens, so the pre-restart poller
	// is guaranteed to die (be discarded) holding an executed-but-unacked
	// command.
	var allowAcks atomic.Bool
	hubHTTP := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/v1/crossnode/commands/") && strings.HasSuffix(r.URL.Path, "/ack") && !allowAcks.Load() {
			http.Error(w, "injected ack outage until restart", http.StatusServiceUnavailable)
			return
		}
		hubHandler.ServeHTTP(w, r)
	}))
	t.Cleanup(hubHTTP.Close)

	t.Setenv(api.EnvNodeID, "spoke-r")
	localHTTP := httptest.NewServer(api.New(localPool, logger).Handler())
	t.Cleanup(localHTTP.Close)

	const originKey = "restart-recovery-create-1"
	enqueueBody := []byte(`{
		"command_path":"/v1/work-items",
		"command_body":{
			"title":"restart recovery item",
			"body":"must exist exactly once after a poller restart"
		}
	}`)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, hubHTTP.URL+"/v1/crossnode/commands", bytes.NewReader(enqueueBody))
	if err != nil {
		t.Fatalf("build enqueue request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+hubToken)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(crossnode.HeaderIdempotencyKey, originKey)
	req.Header.Set(crossnode.HeaderQueueFor, "spoke-r")
	req.Header.Set(crossnode.HeaderTargetNode, "hub")
	req.Header.Set(crossnode.HeaderOriginNode, "hub")
	resp, err := hubHTTP.Client().Do(req)
	if err != nil {
		t.Fatalf("enqueue request: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("enqueue: status=%d body=%s", resp.StatusCode, body)
	}

	config := spoke.Config{
		HubBaseURL: hubHTTP.URL,
		NodeID:     "spoke-r",
		HubToken:   hubToken,
		LocalURL:   localHTTP.URL,
		LocalToken: localToken,
		DrainLimit: 10,
	}

	// Pre-restart poller: executes the command locally, cannot ack.
	preRestart := spoke.New(config, &http.Client{Timeout: 2 * time.Second}, nil, logger)
	first := preRestart.Tick(ctx)
	if !first.HubReachable || first.Drained != 1 || first.Executed != 1 || first.Acked != 0 {
		t.Fatalf("pre-restart tick = %+v, want reachable drained=1 executed=1 acked=0", first)
	}
	assertCount(t, localPool, "SELECT count(*) FROM work_items", 1)
	assertEventCount(t, localPool, domain.EventWorkItemCreated, 1)
	assertCount(t, hubPool, "SELECT count(*) FROM command_queue WHERE target_node_id = 'spoke-r' AND state = 'pending'", 1)

	// The restart: preRestart is discarded here and never ticked again. The
	// hub outage ends, and a brand-new poller — fresh struct, fresh HTTP
	// client, no shared memory with the first — resumes from durable state
	// alone.
	allowAcks.Store(true)
	postRestart := spoke.New(config, &http.Client{Timeout: 2 * time.Second}, nil, logger)
	second := postRestart.Tick(ctx)
	if !second.HubReachable || second.Drained != 1 || second.Executed != 1 || second.Acked != 1 {
		t.Fatalf("post-restart tick = %+v, want reachable drained=1 executed=1 acked=1", second)
	}

	// Exactly one local mutation across both lives: the replay collapsed on
	// the original idempotency key and returned the cached 201.
	assertCount(t, localPool, "SELECT count(*) FROM work_items", 1)
	assertEventCount(t, localPool, domain.EventWorkItemCreated, 1)
	assertEventCount(t, localPool, domain.EventIdempotencyRecorded, 1)
	assertLocalIdempotencyRecord(t, localPool, originKey)
	assertCount(t, hubPool, "SELECT count(*) FROM command_queue WHERE target_node_id = 'spoke-r' AND state = 'done' AND outcome_status_code = 201 AND outcome_ok", 1)
}
