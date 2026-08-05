// `meristem spoke` runs the outbound poll loop of a pull-only fleet node
// (docs/network-layer-spec.md §2 / §6 stage 1). Each tick it drains this node's
// durable command queue on the hub, executes each command against its own local
// api under the spoke-local agent token with the original idempotency key, acks
// the outcome, then advances a persisted hub-feed cursor. Nothing listens; the
// hub being unreachable is logged and retried, never fatal — the node keeps full
// local function during a partition.
//
// Config is entirely environment-driven (tokens never a file path, never
// logged):
//
//	MERISTEM_HUB_URL      base URL of the hub to poll (required)
//	MERISTEM_NODE_ID      this node's DNS-safe node id (required)
//	MERISTEM_HUB_TOKEN    hub-minted bearer for this spoke agent (required)
//	MERISTEM_TOKEN        this node's local agent bearer (required)
//	MERISTEM_LOCAL_URL    local api base URL (default http://localhost:8080)
//	MERISTEM_DATABASE_URL local Postgres DSN (for the feed-cursor bookmark)
//
// With --multi-peer the drain set comes from the nodes projection instead of
// MERISTEM_HUB_URL, and each queue host is reached under its own bearer from
// MERISTEM_PEER_TOKEN_<NODE_ID>. There is no fallback to MERISTEM_HUB_TOKEN:
// bearers are node-local, so presenting one host's token to another both fails
// and hands that host a credential it was never meant to see.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/buildguard"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/spoke"
	"github.com/jbmopper/meristem/internal/storage"
)

// defaultSpokeInterval is the poll cadence when --interval is not supplied.
// §5 budgets a pull-only command at <= the target poll interval (default 30s).
const defaultSpokeInterval = 30 * time.Second

func runSpoke(ctx context.Context, logger *slog.Logger, args []string, build buildguard.StatusProvider) error {
	fs := flag.NewFlagSet("spoke", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var intervalText string
	var multiPeer bool
	var graceText string
	fs.StringVar(&intervalText, "interval", "", "duration between spoke poll ticks (default 30s)")
	fs.BoolVar(&multiPeer, "multi-peer", false, "drain every approved queue host from the nodes projection instead of only MERISTEM_HUB_URL")
	fs.StringVar(&graceText, "drain-grace", "", "how long to keep draining a queue host after it leaves this node's allowlist (default 24h)")
	if err := fs.Parse(args); err != nil {
		spokeUsage(os.Stderr)
		return err
	}
	if fs.NArg() > 0 {
		spokeUsage(os.Stderr)
		return fmt.Errorf("spoke: unexpected argument %q", fs.Arg(0))
	}

	interval := defaultSpokeInterval
	if intervalText != "" {
		parsed, err := time.ParseDuration(intervalText)
		if err != nil {
			return fmt.Errorf("spoke: invalid --interval %q: %w", intervalText, err)
		}
		if parsed <= 0 {
			return fmt.Errorf("spoke: --interval must be positive, got %s", parsed)
		}
		interval = parsed
	}

	grace := time.Duration(0)
	if graceText != "" {
		parsed, err := time.ParseDuration(graceText)
		if err != nil {
			return fmt.Errorf("spoke: invalid --drain-grace %q: %w", graceText, err)
		}
		if parsed < 0 {
			return fmt.Errorf("spoke: --drain-grace must not be negative, got %s", parsed)
		}
		grace = parsed
	}
	if graceText != "" && !multiPeer {
		return fmt.Errorf("spoke: --drain-grace only applies with --multi-peer")
	}

	cfg, err := spoke.LoadConfig()
	if err != nil {
		return err
	}

	// The pool backs only the feed-cursor bookmark (spoke_state); commands run
	// over HTTP against the local api, never the DB directly.
	storageCfg, err := storage.LoadConfigFromEnv()
	if err != nil {
		return err
	}
	pool, err := storage.Open(ctx, storageCfg)
	if err != nil {
		return err
	}
	defer pool.Close()

	writer := app.NewGuardedEventWriter(build)
	localActor, err := auth.NewService(pool, writer).Authenticate(ctx, cfg.LocalToken)
	if err != nil {
		return fmt.Errorf("spoke: authenticate local token for event attribution: %w", err)
	}
	if localActor.IsRoot || localActor.Source != domain.SourceAgent {
		return fmt.Errorf("spoke: %s must resolve to a dedicated non-root agent token", spoke.EnvLocalToken)
	}
	cursor := spoke.NewEventCursorStore(pool, writer, cfg.HubBaseURL, localActor.ID, localActor.Source)
	client := &http.Client{
		Timeout: 30 * time.Second,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	checkBuild := func() error { return buildguard.RequireNonBlocking(build) }
	poller := spoke.NewWithCheck(cfg, client, cursor, logger, checkBuild)

	// Multi-peer draining is opt-in. Without the flag the poller keeps draining
	// exactly MERISTEM_HUB_URL under MERISTEM_HUB_TOKEN, so an existing
	// deployment upgrades with no configuration change and no behavior change.
	if multiPeer {
		peers, err := spoke.NewProjectionPeerSource(pool, cfg.NodeID, grace)
		if err != nil {
			return err
		}
		poller = poller.WithPeers(peers, spoke.EnvBearerResolver())
	}

	logger.Info("spoke poller starting",
		slog.String("node_id", cfg.NodeID),
		slog.String("hub_url", cfg.HubBaseURL),
		slog.String("local_url", cfg.LocalURL),
		slog.String("interval", interval.String()),
		slog.Bool("multi_peer", multiPeer),
	)
	return runCheckedIntervalLoop(
		ctx,
		interval,
		checkBuild,
		func() error {
			_, err := poller.TickChecked(ctx)
			return err
		},
		nil,
	)
}

func spokeUsage(w io.Writer) {
	fmt.Fprint(w, `usage:
  MERISTEM_HUB_URL=https://hub MERISTEM_NODE_ID=m4 \
    MERISTEM_HUB_TOKEN=mrs_<hub-minted> MERISTEM_TOKEN=mrs_<local-agent> \
    meristem spoke [--interval=DURATION]

Runs the outbound poll loop for a pull-only fleet node: it runs one tick
immediately, then repeats every --interval until SIGINT or SIGTERM. Each tick
drains this node's durable command queue on the hub, executes each command
against the local api (MERISTEM_LOCAL_URL, default http://localhost:8080) under
MERISTEM_TOKEN with the original idempotency key, acks the outcome to the hub,
then advances a persisted hub-feed cursor. The hub being unreachable is logged
and retried, never fatal.

  --interval=DURATION    interval between poll ticks. Default: 30s.
  --multi-peer           drain every queue host this node is approved to use,
                         read from the nodes projection each tick, instead of
                         only MERISTEM_HUB_URL. Each host is reached under its
                         own MERISTEM_PEER_TOKEN_<NODE_ID> bearer; a host with
                         no configured credential is skipped, never given
                         another host's token. One unreachable host does not
                         block draining from the others.
  --drain-grace=DURATION with --multi-peer, how long to keep draining a queue
                         host after an operator removes it from this node's
                         allowlist. Default: 24h. Commands already accepted by
                         that host would otherwise be stranded there with
                         nothing coming to collect them.

environment:
  MERISTEM_HUB_URL      base URL of the hub to poll (required)
  MERISTEM_NODE_ID      this node's DNS-safe node id (required)
  MERISTEM_HUB_TOKEN    hub-minted bearer for this spoke agent (required, never logged)
  MERISTEM_TOKEN        this node's local agent bearer (required, never logged)
  MERISTEM_LOCAL_URL    local api base URL (default http://localhost:8080)
  MERISTEM_DATABASE_URL local Postgres DSN (for the feed-cursor bookmark)
  MERISTEM_PEER_TOKEN_<NODE_ID>
                        with --multi-peer, the bearer for one queue host, e.g.
                        MERISTEM_PEER_TOKEN_HUB for peer "hub" and
                        MERISTEM_PEER_TOKEN_HOME_SERVER for "home-server"
                        (required, never logged)
`)
}
