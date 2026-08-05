// `meristem node` is the operator's surface for the fleet node registry
// (docs/network-layer-spec.md §6, stage 1). It appends the node.registered and
// node.route_updated events that internal/nodes folds into the `nodes`
// projection, and prints that projection back for verification.
//
// Like `seed` and `tokens`, it connects straight to Postgres via
// MERISTEM_DATABASE_URL and the shared event writer — it never calls the live
// HTTP API. Writes are attributed to a dedicated system-source token supplied
// in MERISTEM_TOKEN (fleet configuration is a system-internal flow, not a root
// action), matching how `seed v1` resolves its actor.
//
//	meristem node register --node-id m4 --base-url URL [--direct-url URL]
//	                       [--queue-via ID ...] [--status active]
//	meristem node update-route --node-id m4 [--direct-url URL]
//	                           [--queue-via ID ...] [--status active]
//	meristem node list
//	meristem node sync-registry [--once] [--interval 30s]
//	meristem node sync-outcomes [--once] [--interval 30s]
//
// register appends a node.registered event whose payload carries the full
// declared state; an identical re-run collapses onto the same event (the
// payload is the idempotency key, as in seed.go) while any changed field
// appends a fresh event. update-route declares the full replacement
// reachability state (direct_url, queue_via, status): repeating the current
// declaration is a no-op, while returning to an earlier state appends a new
// node.route_updated event. Fields the operator omits are cleared, per the
// projector's replacement contract. list is read-only.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/buildguard"
	"github.com/jbmopper/meristem/internal/crossnode"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/nodes"
	"github.com/jbmopper/meristem/internal/peerhttp"
	"github.com/jbmopper/meristem/internal/storage"
)

const (
	envRegistryHomeNodeID         = "MERISTEM_REGISTRY_HOME_NODE_ID"
	envRegistryHomeOrigin         = "MERISTEM_REGISTRY_HOME_URL"
	envRegistryHomeToken          = "MERISTEM_REGISTRY_HOME_TOKEN"
	envOutcomeQueueHostNodeID     = "MERISTEM_QUEUE_HOST_NODE_ID"
	envOutcomeQueueHostOrigin     = "MERISTEM_QUEUE_HOST_URL"
	envOutcomeQueueHostToken      = "MERISTEM_QUEUE_HOST_OUTCOME_TOKEN"
	defaultRegistrySyncInterval   = 30 * time.Second
	defaultRegistryRequestTimeout = 5 * time.Second
)

func runNode(ctx context.Context, logger *slog.Logger, args []string, build buildguard.StatusProvider) error {
	if len(args) == 0 {
		nodeUsage(os.Stderr)
		return fmt.Errorf("node: missing subcommand")
	}

	cfg, err := storage.LoadConfigFromEnv()
	if err != nil {
		return err
	}
	pool, err := storage.Open(ctx, cfg)
	if err != nil {
		return err
	}
	defer pool.Close()

	writer := app.NewGuardedEventWriter(build)
	checkBuild := func() error { return buildguard.RequireNonBlocking(build) }

	switch args[0] {
	case "register", "update-route":
		actor, err := resolveNodeSystemActor(ctx, auth.NewService(pool, writer))
		if err != nil {
			return err
		}
		if args[0] == "register" {
			return registerNode(ctx, pool, writer, actor, args[1:])
		}
		return updateNodeRoute(ctx, pool, writer, actor, args[1:])
	case "list":
		return listNodes(ctx, pool, os.Stdout, args[1:])
	case "sync-registry":
		return syncRegistryNodeWithCheck(ctx, logger, pool, writer, auth.NewService(pool, writer), checkBuild, args[1:])
	case "sync-outcomes":
		return syncOutcomeNodeWithCheck(ctx, logger, pool, writer, auth.NewService(pool, writer), checkBuild, args[1:])
	default:
		logger.Error("unknown node subcommand", slog.String("subcommand", args[0]))
		nodeUsage(os.Stderr)
		return fmt.Errorf("node: unknown subcommand %q", args[0])
	}
}

func syncOutcomeNode(ctx context.Context, logger *slog.Logger, pool *pgxpool.Pool, writer *events.Writer, authenticator tokenAuthenticator, args []string) error {
	return syncOutcomeNodeWithCheck(ctx, logger, pool, writer, authenticator, nil, args)
}

func syncOutcomeNodeWithCheck(ctx context.Context, logger *slog.Logger, pool *pgxpool.Pool, writer *events.Writer, authenticator tokenAuthenticator, checkBuild func() error, args []string) error {
	fs := flag.NewFlagSet("node sync-outcomes", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	once := fs.Bool("once", false, "perform one queue-outcome reconciliation tick and exit")
	interval := fs.Duration("interval", defaultRegistrySyncInterval, "retry/poll interval")
	requestTimeout := fs.Duration("request-timeout", defaultRegistryRequestTimeout, "timeout for one queue-host request")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || *interval <= 0 || *requestTimeout <= 0 {
		return fmt.Errorf("node sync-outcomes: arguments and durations must be valid")
	}
	queueHostNodeID := strings.TrimSpace(os.Getenv(envOutcomeQueueHostNodeID))
	queueHostOrigin := strings.TrimSpace(os.Getenv(envOutcomeQueueHostOrigin))
	originNodeID := strings.TrimSpace(os.Getenv("MERISTEM_NODE_ID"))
	remoteToken := os.Getenv(envOutcomeQueueHostToken)
	localSecret := os.Getenv("MERISTEM_TOKEN")
	if queueHostNodeID == "" || queueHostOrigin == "" || originNodeID == "" || remoteToken == "" || localSecret == "" {
		return fmt.Errorf("node sync-outcomes: %s, %s, %s, MERISTEM_NODE_ID, and MERISTEM_TOKEN are required", envOutcomeQueueHostNodeID, envOutcomeQueueHostOrigin, envOutcomeQueueHostToken)
	}
	actor, err := authenticator.Authenticate(ctx, localSecret)
	if err != nil {
		return fmt.Errorf("node sync-outcomes: authenticate local observer: %w", err)
	}
	service, err := crossnode.NewOutcomeSyncService(crossnode.NewOutcomeObserver(pool, writer), crossnode.OutcomeSyncConfig{
		QueueHostNodeID: queueHostNodeID,
		QueueHostOrigin: queueHostOrigin,
		OriginNodeID:    originNodeID,
		QueueHostToken:  remoteToken,
		LocalActor:      actor,
		RequestTimeout:  *requestTimeout,
	}, peerhttp.Options{})
	if err != nil {
		return fmt.Errorf("node sync-outcomes: %w", err)
	}
	tick := func() error {
		result, err := service.Tick(ctx)
		if err != nil {
			return err
		}
		logger.Info("queue outcome reconciliation complete",
			slog.String("queue_host_node_id", queueHostNodeID),
			slog.String("origin_node_id", originNodeID),
			slog.Int64("remote_event_seq", result.Cursor),
			slog.Int("observed", result.Observed),
			slog.Int("cause_transitions", result.CauseTransitions))
		return nil
	}
	if *once {
		return runCheckedTick(checkBuild, tick)
	}
	return runCheckedIntervalLoop(ctx, *interval, checkBuild, tick, func(err error) {
		logger.Warn("queue outcome reconciliation failed; retaining cursor",
			slog.String("queue_host_node_id", queueHostNodeID),
			slog.String("retry_in", interval.String()),
			slog.String("error", err.Error()))
	})
}

// syncRegistryNode reconciles the registry home into this node's own event log
// using outbound-only authenticated REST. A failed tick leaves the most recent
// observed snapshot untouched and the daemon retries after a finite interval.
func syncRegistryNode(ctx context.Context, logger *slog.Logger, pool *pgxpool.Pool, writer *events.Writer, authenticator tokenAuthenticator, args []string) error {
	return syncRegistryNodeWithCheck(ctx, logger, pool, writer, authenticator, nil, args)
}

func syncRegistryNodeWithCheck(ctx context.Context, logger *slog.Logger, pool *pgxpool.Pool, writer *events.Writer, authenticator tokenAuthenticator, checkBuild func() error, args []string) error {
	fs := flag.NewFlagSet("node sync-registry", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	once := fs.Bool("once", false, "perform one registry reconciliation tick and exit")
	interval := fs.Duration("interval", defaultRegistrySyncInterval, "retry/poll interval")
	requestTimeout := fs.Duration("request-timeout", defaultRegistryRequestTimeout, "timeout for one registry-home request")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("node sync-registry: unexpected argument %q", fs.Arg(0))
	}
	if *interval <= 0 {
		return fmt.Errorf("node sync-registry: --interval must be positive")
	}
	if *requestTimeout <= 0 {
		return fmt.Errorf("node sync-registry: --request-timeout must be positive")
	}

	expectedSource := strings.TrimSpace(os.Getenv(envRegistryHomeNodeID))
	origin := strings.TrimSpace(os.Getenv(envRegistryHomeOrigin))
	remoteToken := os.Getenv(envRegistryHomeToken)
	localSecret := os.Getenv("MERISTEM_TOKEN")
	if expectedSource == "" || origin == "" || remoteToken == "" || localSecret == "" {
		return fmt.Errorf("node sync-registry: %s, %s, %s, and MERISTEM_TOKEN are required", envRegistryHomeNodeID, envRegistryHomeOrigin, envRegistryHomeToken)
	}
	actor, err := authenticator.Authenticate(ctx, localSecret)
	if err != nil {
		return fmt.Errorf("node sync-registry: authenticate local observer: %w", err)
	}
	service, err := nodes.NewRegistrySyncService(nodes.NewSnapshotService(pool, writer), nodes.RegistrySyncConfig{
		RegistryHomeOrigin: origin,
		ExpectedSource:     expectedSource,
		RegistryHomeToken:  remoteToken,
		LocalActor:         actor,
		RequestTimeout:     *requestTimeout,
	}, peerhttp.Options{})
	if err != nil {
		return fmt.Errorf("node sync-registry: %w", err)
	}

	tick := func() error {
		result, err := service.Tick(ctx)
		if err != nil {
			return err
		}
		logger.Info("registry snapshot reconciliation complete",
			slog.String("registry_home_node_id", expectedSource),
			slog.Int64("source_revision", result.SourceRevision),
			slog.Bool("observed", result.Observed),
		)
		return nil
	}
	if *once {
		return runCheckedTick(checkBuild, tick)
	}
	return runCheckedIntervalLoop(ctx, *interval, checkBuild, tick, func(err error) {
		logger.Warn("registry snapshot reconciliation failed; retaining last accepted snapshot",
			slog.String("registry_home_node_id", expectedSource),
			slog.String("retry_in", interval.String()),
			slog.String("error", err.Error()),
		)
	})
}

// runCheckedTick performs the dynamic build check immediately before a tick.
// Runtime callers pass a reviewed-v1 guard; direct integration helpers may
// omit it so their existing signatures remain stable.
func runCheckedTick(checkBuild func() error, tick func() error) error {
	if checkBuild != nil {
		if err := checkBuild(); err != nil {
			return err
		}
	}
	return tick()
}

// runCheckedIntervalLoop preserves the node/spoke immediate-first-tick
// cadence while making a build-pin change terminal for this process. Ordinary
// transport failures still follow the caller's bounded retry policy.
func runCheckedIntervalLoop(ctx context.Context, interval time.Duration, checkBuild func() error, tick func() error, onTickError func(error)) error {
	if interval <= 0 {
		return fmt.Errorf("tick interval must be positive, got %s", interval)
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if checkBuild != nil {
			if err := checkBuild(); err != nil {
				return err
			}
		}
		if err := tick(); err != nil {
			if errors.Is(err, buildguard.ErrBlocked) {
				return err
			}
			if onTickError != nil {
				onTickError(err)
			}
		}

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

// registerNode parses register flags, builds the node.registered payload, and
// appends it. Split from runNode (which owns env + pool wiring) so an
// integration test can drive it against a pgtest pool with an injected actor,
// mirroring seed.go's runSeedV1/seedV1Items split.
func registerNode(ctx context.Context, pool *pgxpool.Pool, writer *events.Writer, actor domain.Token, args []string) error {
	fs := flag.NewFlagSet("node register", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	nodeID := fs.String("node-id", "", "DNS-safe fleet node id (required)")
	statusFlag := fs.String("status", string(domain.NodeStatusActive), "reachability status")
	var baseURL, directURL optionalStringFlag
	var queueVia stringSliceFlag
	fs.Var(&baseURL, "base-url", "registered ingress base URL (optional)")
	fs.Var(&directURL, "direct-url", "direct peer route URL (optional)")
	fs.Var(&queueVia, "queue-via", "queue-host node id; repeatable")
	fs.Var(&queueVia, "relay-via", "deprecated alias for --queue-via")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *nodeID == "" {
		return fmt.Errorf("node register: --node-id is required")
	}
	base, err := baseURL.nonEmptyPtr("base-url")
	if err != nil {
		return fmt.Errorf("node register: %w", err)
	}
	direct, err := directURL.nonEmptyPtr("direct-url")
	if err != nil {
		return fmt.Errorf("node register: %w", err)
	}

	payload, err := nodes.BuildRegisteredPayload(nodes.RegisterParams{
		NodeID:    *nodeID,
		BaseURL:   base,
		DirectURL: direct,
		QueueVia:  queueVia,
		Status:    *statusFlag,
	})
	if err != nil {
		return fmt.Errorf("node register: %w", err)
	}

	fresh, err := appendNodeEvent(ctx, pool, writer, actor, nodes.NodeSubjectID(*nodeID), domain.EventNodeRegistered, payload)
	if err != nil {
		return fmt.Errorf("node register: %w", err)
	}
	fmt.Fprintf(os.Stdout, "node register: node_id=%s %s\n", *nodeID, freshLabel(fresh, "registered"))
	return nil
}

// updateNodeRoute parses update-route flags and appends a node.route_updated
// event carrying the full replacement route state. Omitted --direct-url and
// --queue-via clear those columns; that is the projector's contract, not a bug.
func updateNodeRoute(ctx context.Context, pool *pgxpool.Pool, writer *events.Writer, actor domain.Token, args []string) error {
	fs := flag.NewFlagSet("node update-route", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	nodeID := fs.String("node-id", "", "DNS-safe fleet node id (required)")
	statusFlag := fs.String("status", string(domain.NodeStatusActive), "reachability status")
	var directURL optionalStringFlag
	var queueVia stringSliceFlag
	fs.Var(&directURL, "direct-url", "direct peer route URL (optional; omitted clears it)")
	fs.Var(&queueVia, "queue-via", "queue-host node id; repeatable (omitted clears the list)")
	fs.Var(&queueVia, "relay-via", "deprecated alias for --queue-via")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *nodeID == "" {
		return fmt.Errorf("node update-route: --node-id is required")
	}
	direct, err := directURL.nonEmptyPtr("direct-url")
	if err != nil {
		return fmt.Errorf("node update-route: %w", err)
	}

	payload, err := nodes.BuildRouteUpdatedPayload(nodes.RouteParams{
		NodeID:    *nodeID,
		DirectURL: direct,
		QueueVia:  queueVia,
		Status:    *statusFlag,
	})
	if err != nil {
		return fmt.Errorf("node update-route: %w", err)
	}

	fresh, err := appendNodeRouteEvent(ctx, pool, writer, actor, *nodeID, payload)
	if err != nil {
		return fmt.Errorf("node update-route: %w", err)
	}
	fmt.Fprintf(os.Stdout, "node update-route: node_id=%s %s\n", *nodeID, freshLabel(fresh, "updated"))
	return nil
}

// appendNodeRouteEvent serializes declarations for one node against its
// current projection. Repeating the current desired route is a no-op, while a
// later A -> B -> A declaration receives the immediately preceding node event
// as its discriminator and therefore cannot collapse onto the earlier A.
// Locking the node row makes the comparison/discriminator pair safe when two
// node-admin processes update the same node concurrently.
func appendNodeRouteEvent(ctx context.Context, pool *pgxpool.Pool, writer *events.Writer, actor domain.Token, nodeID string, payload any) (bool, error) {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var (
		currentDirect *string
		currentRelay  []byte
		currentStatus string
	)
	// relay_via, not queue_via, for the expand window — see nodes.List. Reading
	// the new column here would compare the desired route against a stale value
	// and wrongly collapse a real change into a no-op.
	if err := tx.QueryRow(ctx, `
		SELECT direct_url, relay_via, status
		FROM nodes
		WHERE node_id = $1
		FOR UPDATE
	`, nodeID).Scan(&currentDirect, &currentRelay, &currentStatus); err != nil {
		if err == pgx.ErrNoRows {
			return false, fmt.Errorf("node %q is not registered", nodeID)
		}
		return false, fmt.Errorf("lock node route: %w", err)
	}

	var desired struct {
		DirectURL *string  `json:"direct_url"`
		QueueVia  []string `json:"queue_via"`
		Status    string   `json:"status"`
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return false, fmt.Errorf("marshal route payload: %w", err)
	}
	if err := json.Unmarshal(b, &desired); err != nil {
		return false, fmt.Errorf("decode route payload: %w", err)
	}
	var relay []string
	if err := json.Unmarshal(currentRelay, &relay); err != nil {
		return false, fmt.Errorf("decode current queue_via: %w", err)
	}
	if optionalStringsEqual(currentDirect, desired.DirectURL) && stringSlicesEqual(relay, desired.QueueVia) && currentStatus == desired.Status {
		return false, nil
	}

	var predecessor uuid.UUID
	if err := tx.QueryRow(ctx, `
		SELECT id
		FROM events
		WHERE subject_kind = $1 AND subject_id = $2
		ORDER BY seq DESC
		LIMIT 1
	`, domain.SubjectNode, nodes.NodeSubjectID(nodeID)).Scan(&predecessor); err != nil {
		return false, fmt.Errorf("resolve route predecessor: %w", err)
	}

	_, fresh, err := writer.Append(ctx, tx, events.Spec{
		SubjectKind:   domain.SubjectNode,
		SubjectID:     nodes.NodeSubjectID(nodeID),
		Kind:          domain.EventNodeRouteUpdated,
		Source:        domain.SourceSystem,
		ActorTokenID:  &actor.ID,
		Discriminator: "predecessor:" + predecessor.String(),
		Payload:       payload,
	})
	if err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return fresh, nil
}

func optionalStringsEqual(a, b *string) bool {
	return (a == nil && b == nil) || (a != nil && b != nil && *a == *b)
}

func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// listNodes prints the nodes projection: one tab-separated row per node with
// node_id, base_url, direct_url, queue_via, status, updated_at. Absent URLs
// and an empty queue_via render as "-".
func listNodes(ctx context.Context, pool *pgxpool.Pool, w io.Writer, args []string) error {
	fs := flag.NewFlagSet("node list", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	if err := fs.Parse(args); err != nil {
		return err
	}
	rows, err := nodes.List(ctx, pool)
	if err != nil {
		return err
	}
	for _, n := range rows {
		relay := "-"
		if len(n.QueueVia) > 0 {
			relay = strings.Join(n.QueueVia, ",")
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n",
			n.NodeID,
			valueOrDash(n.BaseURL),
			valueOrDash(n.DirectURL),
			relay,
			n.Status,
			n.UpdatedAt.Format(time.RFC3339),
		)
	}
	return nil
}

// appendNodeEvent appends one node event in its own transaction and reports
// whether it was fresh (a new row) or a collapsed replay. The discriminator is
// intentionally empty: the full-state payload is the idempotency key, exactly
// as in seed.go — an identical re-run collapses, a changed field appends.
func appendNodeEvent(ctx context.Context, pool *pgxpool.Pool, writer *events.Writer, actor domain.Token, subjectID uuid.UUID, kind string, payload any) (bool, error) {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	_, fresh, err := writer.Append(ctx, tx, events.Spec{
		SubjectKind:  domain.SubjectNode,
		SubjectID:    subjectID,
		Kind:         kind,
		Source:       domain.SourceSystem,
		ActorTokenID: &actor.ID,
		Payload:      payload,
	})
	if err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return fresh, nil
}

// resolveNodeSystemActor loads the bearer in MERISTEM_TOKEN and requires a
// dedicated, non-root system token. Fleet-node writes are a system-internal
// flow; like `seed v1`, they refuse root and refuse a non-system source so the
// audit attributes them cleanly.
func resolveNodeSystemActor(ctx context.Context, service tokenAuthenticator) (domain.Token, error) {
	secret := os.Getenv("MERISTEM_TOKEN")
	if secret == "" {
		return domain.Token{}, fmt.Errorf("node: MERISTEM_TOKEN with a system-source bearer is required (mint one with `meristem tokens create --source system --name node-admin`)")
	}
	tok, err := service.Authenticate(ctx, secret)
	if err != nil {
		return domain.Token{}, err
	}
	if tok.Source != domain.SourceSystem {
		return domain.Token{}, fmt.Errorf("node: MERISTEM_TOKEN must be source=system, got %q (root is deliberately not accepted)", tok.Source)
	}
	if tok.IsRoot {
		return domain.Token{}, fmt.Errorf("node: MERISTEM_TOKEN must be a dedicated system token, not root")
	}
	return tok, nil
}

func freshLabel(fresh bool, appended string) string {
	if fresh {
		return appended
	}
	return "unchanged"
}

func valueOrDash(p *string) string {
	if p == nil || *p == "" {
		return "-"
	}
	return *p
}

// optionalStringFlag distinguishes "flag not provided" (nil pointer, field
// omitted from the payload) from "flag provided". flag.String cannot express
// that because an unset string flag is indistinguishable from --flag="".
type optionalStringFlag struct {
	set   bool
	value string
}

func (o *optionalStringFlag) String() string { return o.value }

func (o *optionalStringFlag) Set(v string) error {
	o.value = v
	o.set = true
	return nil
}

// nonEmptyPtr returns nil when the flag was never set, the value when it was
// set to a non-empty string, and an error when it was set to an empty/blank
// string (an explicit empty URL is a mistake, not "omit it").
func (o *optionalStringFlag) nonEmptyPtr(name string) (*string, error) {
	if !o.set {
		return nil, nil
	}
	if strings.TrimSpace(o.value) == "" {
		return nil, fmt.Errorf("--%s was provided but empty", name)
	}
	v := o.value
	return &v, nil
}

// stringSliceFlag collects a repeatable flag into a slice, preserving order.
type stringSliceFlag []string

func (s *stringSliceFlag) String() string { return strings.Join(*s, ",") }

func (s *stringSliceFlag) Set(v string) error {
	*s = append(*s, v)
	return nil
}

func nodeUsage(w io.Writer) {
	fmt.Fprint(w, `usage:
  MERISTEM_TOKEN=mrs_<system> meristem node register --node-id ID [--base-url URL] [--direct-url URL] [--queue-via ID ...] [--status active]
  MERISTEM_TOKEN=mrs_<system> meristem node update-route --node-id ID [--direct-url URL] [--queue-via ID ...] [--status active]
  meristem node list
  MERISTEM_REGISTRY_HOME_URL=https://registry.example \
    MERISTEM_REGISTRY_HOME_NODE_ID=registry \
    MERISTEM_REGISTRY_HOME_TOKEN=mrs_<home-read> \
    MERISTEM_TOKEN=mrs_<local-observer> \
    meristem node sync-registry [--once] [--interval=30s] [--request-timeout=5s]
  MERISTEM_QUEUE_HOST_URL=https://queue.example \
    MERISTEM_QUEUE_HOST_NODE_ID=queue-host \
    MERISTEM_QUEUE_HOST_OUTCOME_TOKEN=mrs_<origin-read> \
    MERISTEM_NODE_ID=origin MERISTEM_TOKEN=mrs_<local-observer> \
    meristem node sync-outcomes [--once] [--interval=30s] [--request-timeout=5s]

Appends node.registered / node.route_updated events to the fleet node registry
and prints the resulting projection. Connects directly via MERISTEM_DATABASE_URL;
writes require a system-source MERISTEM_TOKEN. Re-running an identical
registration is a no-op; a changed field appends a new event. update-route fully
replaces the route state (direct_url, queue_via, status) — omitted fields clear.

sync-registry performs authenticated outbound GETs against the pinned registry
home and appends validated snapshots to this node's own log. It never pushes to
the consumer. Without --once it retries forever at the finite --interval; an
outage retains the last accepted snapshot. Bearers are required and never logged.

sync-outcomes polls a queue host for terminal commands whose immutable origin
matches MERISTEM_NODE_ID. It records local observed-outcome events and advances
an event-backed cursor; an observed expiry fails only a non-terminal causing
work item homed on that origin. Queue-host outages retain outcomes and cursor.
`)
}
