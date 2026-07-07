package spoke

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/crossnode"
)

// maxResponseBody bounds how much of a hub/local response the spoke buffers.
// Cross-node responses are small structured JSON; this only guards a
// misbehaving peer, not a real payload limit.
const maxResponseBody = 1 << 20

// Poller runs the spoke's outbound loop: drain the command queue, then advance
// the hub-feed cursor. Construct it with New and drive one iteration with Tick.
type Poller struct {
	cfg    Config
	client *http.Client
	cursor CursorStore
	logger *slog.Logger
}

// New constructs a Poller. A nil client uses http.DefaultClient; a nil logger
// uses slog.Default.
func New(cfg Config, client *http.Client, cursor CursorStore, logger *slog.Logger) *Poller {
	if client == nil {
		client = http.DefaultClient
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Poller{cfg: cfg, client: client, cursor: cursor, logger: logger}
}

// TickResult reports what one Tick did, for logging and tests.
type TickResult struct {
	// Drained is how many pending commands the hub returned.
	Drained int
	// Executed is how many of those produced a definitive local HTTP response
	// (and were therefore acked). A local transport failure is neither executed
	// nor acked — the row stays pending for the next tick.
	Executed int
	// Failed is how many executed commands returned a non-2xx (acked ok=false).
	Failed int
	// Acked is how many acks the hub accepted.
	Acked int
	// NewFeedEvents is how many new hub-feed events this tick observed.
	NewFeedEvents int
	// HubReachable is false when the drain or feed poll could not reach the hub.
	HubReachable bool
}

// Tick runs one full spoke iteration. It never returns an error: a hub that is
// unreachable is logged at warn and the tick ends cleanly so the loop keeps
// running (partition tolerance, §2 "Partition behavior"). Non-hub faults
// (local execution transport errors, cursor persistence) are logged and the
// tick continues; the affected command simply stays pending.
func (p *Poller) Tick(ctx context.Context) TickResult {
	res := TickResult{HubReachable: true}

	if err := p.drain(ctx, &res); err != nil {
		res.HubReachable = false
		p.logger.Warn("spoke drain skipped: hub unreachable, will retry next tick",
			slog.String("node_id", p.cfg.NodeID),
			slog.String("error", err.Error()),
		)
	}

	if err := p.pollFeed(ctx, &res); err != nil {
		res.HubReachable = false
		p.logger.Warn("spoke feed poll skipped: hub unreachable, will retry next tick",
			slog.String("node_id", p.cfg.NodeID),
			slog.String("error", err.Error()),
		)
	}

	p.logger.Debug("spoke tick complete",
		slog.String("node_id", p.cfg.NodeID),
		slog.Int("drained", res.Drained),
		slog.Int("executed", res.Executed),
		slog.Int("failed", res.Failed),
		slog.Int("acked", res.Acked),
		slog.Int("new_feed_events", res.NewFeedEvents),
		slog.Bool("hub_reachable", res.HubReachable),
	)
	return res
}

// drain fetches this node's pending commands from the hub, executes each against
// the local api, and acks the outcome. It returns an error only when the hub
// queue read itself fails (partition); per-command local/ack faults are logged
// and skipped so one bad command never stalls the batch.
func (p *Poller) drain(ctx context.Context, res *TickResult) error {
	commands, err := p.fetchPending(ctx)
	if err != nil {
		return err
	}
	res.Drained = len(commands)

	for _, cmd := range commands {
		status, err := p.executeLocal(ctx, cmd)
		if err != nil {
			// A local transport failure is not a structural outcome: we cannot
			// say the command ran, so we do not ack. The row stays pending and
			// the next tick retries under the same original idempotency key.
			p.logger.Warn("spoke local execution failed, leaving command pending",
				slog.String("node_id", p.cfg.NodeID),
				slog.String("event_id", cmd.EventID.String()),
				slog.String("command_path", cmd.CommandPath),
				slog.String("error", err.Error()),
			)
			continue
		}
		res.Executed++
		ok := status >= 200 && status < 300
		if !ok {
			res.Failed++
		}
		if err := p.ackHub(ctx, cmd.EventID, status, ok); err != nil {
			p.logger.Warn("spoke ack failed, will re-ack next tick",
				slog.String("node_id", p.cfg.NodeID),
				slog.String("event_id", cmd.EventID.String()),
				slog.String("error", err.Error()),
			)
			continue
		}
		res.Acked++
	}
	return nil
}

// fetchPending reads GET /v1/crossnode/commands?target=<node_id>&limit=N from
// the hub under the spoke's hub bearer.
func (p *Poller) fetchPending(ctx context.Context) ([]crossnode.QueuedCommand, error) {
	q := url.Values{}
	q.Set("target", p.cfg.NodeID)
	q.Set("limit", strconv.Itoa(p.cfg.DrainLimit))
	endpoint := p.hubURL("/v1/crossnode/commands") + "?" + q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+p.cfg.HubToken)

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxResponseBody))
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("hub GET commands: status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var parsed struct {
		Commands []crossnode.QueuedCommand `json:"commands"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, fmt.Errorf("hub GET commands: decode: %w", err)
	}
	return parsed.Commands, nil
}

// executeLocal replays the command against this node's own local api under the
// spoke-local agent token, reusing the ORIGINAL idempotency key so a drained
// execution collapses with any direct retry. It returns the HTTP status of a
// definitive response, or an error on transport failure.
func (p *Poller) executeLocal(ctx context.Context, cmd crossnode.QueuedCommand) (int, error) {
	body := cmd.CommandBody
	if len(bytes.TrimSpace(body)) == 0 {
		body = json.RawMessage("{}")
	}
	endpoint := strings.TrimRight(p.cfg.LocalURL, "/") + cmd.CommandPath

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+p.cfg.LocalToken)
	req.Header.Set("Idempotency-Key", cmd.OriginIdempotencyKey)

	resp, err := p.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxResponseBody))
	return resp.StatusCode, nil
}

// ackPayload is the structural outcome posted to the hub ack endpoint.
type ackPayload struct {
	StatusCode int  `json:"status_code"`
	OK         bool `json:"ok"`
}

// ackHub posts the structural outcome to
// POST /v1/crossnode/commands/{event_id}/ack under the hub bearer, keyed by the
// command's event id so a re-ack collapses at the hub's idempotency middleware.
func (p *Poller) ackHub(ctx context.Context, eventID uuid.UUID, status int, ok bool) error {
	body, err := json.Marshal(ackPayload{StatusCode: status, OK: ok})
	if err != nil {
		return err
	}
	endpoint := p.hubURL("/v1/crossnode/commands/" + eventID.String() + "/ack")

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+p.cfg.HubToken)
	req.Header.Set("Idempotency-Key", "ack:"+eventID.String())

	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	ackBody, _ := io.ReadAll(io.LimitReader(resp.Body, maxResponseBody))
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("hub ack: status %d: %s", resp.StatusCode, strings.TrimSpace(string(ackBody)))
	}
	return nil
}

// pollFeed advances the persisted hub-feed cursor by one page and records the
// new-event count. It never re-projects hub events (the remote_refs cache is
// out of scope); the cursor is pure observability plus a resume point.
func (p *Poller) pollFeed(ctx context.Context, res *TickResult) error {
	cursor := ""
	if p.cursor != nil {
		loaded, err := p.cursor.Load(ctx)
		if err != nil {
			// A cursor load fault is local, not a partition. Log and fall back
			// to a from-now bootstrap rather than aborting the feed poll.
			p.logger.Warn("spoke cursor load failed, bootstrapping from hub head",
				slog.String("node_id", p.cfg.NodeID),
				slog.String("error", err.Error()),
			)
		} else {
			cursor = loaded
		}
	}

	q := url.Values{}
	if cursor != "" {
		q.Set("cursor", cursor)
	}
	// wait=0s puts the hub feed in cursor (Page) mode even on the first,
	// cursor-less tick, so it returns a NextCursor "from now" to resume from.
	q.Set("wait", "0s")
	endpoint := p.hubURL("/v1/feed") + "?" + q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+p.cfg.HubToken)

	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxResponseBody))
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("hub GET feed: status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var page struct {
		Items      []json.RawMessage `json:"items"`
		NextCursor string            `json:"next_cursor"`
	}
	if err := json.Unmarshal(body, &page); err != nil {
		return fmt.Errorf("hub GET feed: decode: %w", err)
	}
	res.NewFeedEvents = len(page.Items)

	if page.NextCursor != "" && p.cursor != nil {
		if err := p.cursor.Save(ctx, page.NextCursor); err != nil {
			p.logger.Warn("spoke cursor save failed, next tick may re-observe events",
				slog.String("node_id", p.cfg.NodeID),
				slog.String("error", err.Error()),
			)
		}
	}
	if res.NewFeedEvents > 0 {
		p.logger.Debug("spoke observed new hub feed events",
			slog.String("node_id", p.cfg.NodeID),
			slog.Int("new_feed_events", res.NewFeedEvents),
		)
	}
	return nil
}

func (p *Poller) hubURL(path string) string {
	return strings.TrimRight(p.cfg.HubBaseURL, "/") + path
}

// RunLoop drives Tick every interval until ctx is cancelled, running one tick
// immediately. It returns ctx.Err() on cancellation; individual ticks never
// abort the loop (partition tolerance).
func RunLoop(ctx context.Context, p *Poller, interval time.Duration) error {
	if interval <= 0 {
		return fmt.Errorf("spoke: interval must be positive, got %s", interval)
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		p.Tick(ctx)

		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
}
