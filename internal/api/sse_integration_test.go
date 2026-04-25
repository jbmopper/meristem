package api

// Integration tests for the SSE push stream (a11dd7d5).
//
// Unlike the rest of the integration tests in this package, these need
// a real httptest.NewServer rather than handler.ServeHTTP against a
// ResponseRecorder: ResponseRecorder buffers the entire response, which
// would deadlock SSE (the whole point is to flush frames before the
// handler returns).

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jbmopper/wayline/internal/app"
	"github.com/jbmopper/wayline/internal/auth"
	"github.com/jbmopper/wayline/internal/domain"
	"github.com/jbmopper/wayline/internal/storage"
)

// TestSSEStreamPushesNewEvent: open a stream, write a signal a moment
// later, and verify the corresponding SSE frame arrives. The bound is
// generous (1.5s) but real — the server's poll cadence is 100ms and the
// signal write goes through the regular events writer, so end-to-end
// the frame should land in <300ms typically.
func TestSSEStreamPushesNewEvent(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	tokenResult, err := auth.NewService(pool, app.NewEventWriter()).CreateToken(ctx, auth.CreateTokenInput{
		Name: "sse-push", IsRoot: true, Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create token: %v", err)
	}
	server := New(pool, nil)
	hsrv := httptest.NewServer(server.Handler())
	defer hsrv.Close()

	streamCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	frames := make(chan sseFrameForTest, 8)
	go consumeSSEForTest(t, streamCtx, hsrv.URL+"/v1/feed/stream", tokenResult.Secret, "", frames)

	// Give the stream a moment to attach so we don't race the snapshot.
	time.Sleep(150 * time.Millisecond)

	rec := postSignal(t, server.Handler(), tokenResult.Secret, "sse-push-key-1",
		signalRequestBody(t, "integration:sse-push:1"))
	if rec.Code != http.StatusCreated {
		t.Fatalf("signal create: %d %s", rec.Code, rec.Body.String())
	}

	// A POST to /v1/signals produces multiple visible events (the signal
	// itself plus the projected work_item.created when dedupe didn't
	// hit). The contract this test pins is "the stream pushes events
	// that landed after connect within bounded latency"; any of those
	// events satisfies it.
	deadline := time.After(1500 * time.Millisecond)
	for {
		select {
		case frame, ok := <-frames:
			if !ok {
				t.Fatal("stream closed before any frame arrived")
			}
			if frame.id == "" {
				t.Errorf("frame missing id: %s", frame.data)
			}
			if strings.Contains(frame.data, "signal.received") || strings.Contains(frame.data, "work_item.created") {
				return
			}
			// Some other event slipped through (e.g. token bookkeeping
			// from token creation up top, race-y but possible). Keep
			// looking until deadline.
		case <-deadline:
			t.Fatal("did not receive a signal/work_item SSE frame within 1.5s of the signal write")
		}
	}
}

// TestSSEStreamReplaysFromLastEventID writes two events first, captures
// the cursor of the first, then opens a stream with that cursor as
// Last-Event-ID. The stream MUST emit the second event (and any
// subsequent ones) but NOT the first. This is the resume-after-gap
// contract that lets a watcher reconnect cleanly.
func TestSSEStreamReplaysFromLastEventID(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	tokenResult, err := auth.NewService(pool, app.NewEventWriter()).CreateToken(ctx, auth.CreateTokenInput{
		Name: "sse-replay", IsRoot: true, Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create token: %v", err)
	}
	server := New(pool, nil)
	hsrv := httptest.NewServer(server.Handler())
	defer hsrv.Close()

	rec := postSignal(t, server.Handler(), tokenResult.Secret, "sse-replay-1",
		signalRequestBody(t, "integration:sse-replay:1"))
	if rec.Code != http.StatusCreated {
		t.Fatalf("signal 1: %d %s", rec.Code, rec.Body.String())
	}
	cursorAfterFirst := fetchHeadCursor(t, server.Handler(), tokenResult.Secret)

	rec = postSignal(t, server.Handler(), tokenResult.Secret, "sse-replay-2",
		signalRequestBody(t, "integration:sse-replay:2"))
	if rec.Code != http.StatusCreated {
		t.Fatalf("signal 2: %d %s", rec.Code, rec.Body.String())
	}

	streamCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	frames := make(chan sseFrameForTest, 16)
	go consumeSSEForTest(t, streamCtx, hsrv.URL+"/v1/feed/stream", tokenResult.Secret, cursorAfterFirst, frames)

	deadline := time.After(1500 * time.Millisecond)
	saw := []sseFrameForTest{}
collect:
	for {
		select {
		case f, ok := <-frames:
			if !ok {
				break collect
			}
			saw = append(saw, f)
			if strings.Contains(f.data, "integration:sse-replay:2") {
				// Got the post-cursor event; we have what we need.
				break collect
			}
		case <-deadline:
			t.Fatalf("never saw the post-cursor event in stream; saw %d frames", len(saw))
		}
	}

	for _, f := range saw {
		if strings.Contains(f.data, "integration:sse-replay:1") {
			t.Errorf("pre-cursor event leaked into resumed stream: %s", f.data)
		}
	}
}

// TestSSEStreamRejectsInvalidLastEventID pins the contract: a
// fabricated or stale cursor on stream connect MUST 400, never silently
// degrade to "from now". The same existence-check that protects the
// JSON feed endpoint also protects the stream endpoint.
func TestSSEStreamRejectsInvalidLastEventID(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	tokenResult, err := auth.NewService(pool, app.NewEventWriter()).CreateToken(ctx, auth.CreateTokenInput{
		Name: "sse-reject", IsRoot: true, Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create token: %v", err)
	}
	server := New(pool, nil)
	hsrv := httptest.NewServer(server.Handler())
	defer hsrv.Close()

	fabricated := encodeSeqCursorForTest(99999999)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, hsrv.URL+"/v1/feed/stream", nil)
	if err != nil {
		t.Fatalf("new req: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+tokenResult.Secret)
	req.Header.Set("Last-Event-ID", fabricated)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

// sseFrameForTest is one parsed SSE event for assertions.
type sseFrameForTest struct {
	id   string
	data string
}

// consumeSSEForTest opens a stream and forwards parsed frames to out.
// Closes the channel when the connection ends or ctx cancels. Errors
// are logged via t.Log but don't fail the test (the test asserts on
// what arrived in the channel).
func consumeSSEForTest(t *testing.T, ctx context.Context, url, token, lastID string, out chan<- sseFrameForTest) {
	defer close(out)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		t.Logf("sse new req: %v", err)
		return
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "text/event-stream")
	if lastID != "" {
		req.Header.Set("Last-Event-ID", lastID)
	}
	// http.DefaultClient has no Timeout, exactly what we need for a
	// long-lived stream. Cancellation rides on ctx.
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		if ctx.Err() == nil {
			t.Logf("sse do: %v", err)
		}
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Logf("sse non-200: %d", resp.StatusCode)
		return
	}

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	var (
		curID   string
		curData strings.Builder
	)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if curData.Len() > 0 {
				out <- sseFrameForTest{id: curID, data: curData.String()}
			}
			curData.Reset()
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		field, value, _ := strings.Cut(line, ":")
		value = strings.TrimPrefix(value, " ")
		switch field {
		case "id":
			curID = value
		case "data":
			if curData.Len() > 0 {
				curData.WriteByte('\n')
			}
			curData.WriteString(value)
		}
	}
}

// Compile-time guard: keep imports honest while keeping the test file
// independent of util churn.
var (
	_ = json.Unmarshal
	_ = fmt.Errorf
)
