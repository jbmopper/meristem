// meristem is the single binary for the meristem coordination plane.
//
// Two runtime modes share one codebase and one database, per the spec:
//
//	meristem api               - HTTP surface
//	meristem worker            - deterministic reconciler daemon
//
// Plus operational subcommands:
//
//	meristem migrate  - apply pending Postgres migrations
//	meristem tokens   - mint, list, and revoke bearer tokens
//	meristem mcp      - run the MCP stdio server
//	meristem seed     - seed substrate backlogs into the running system
//	meristem rebuild  - rebuild projections from events into a sandbox schema and diff
//	meristem safety   - validate deterministic resource-safety controls
//	meristem export-context - materialize an allow/deny repo slice (operator-side)
//	meristem git      - run git(1) (passes through all arguments)
//	meristem version  - print build info
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/api"
	"github.com/jbmopper/meristem/internal/policyprofile"
	"github.com/jbmopper/meristem/internal/storage"
)

// version is overridden at build time with -ldflags "-X main.version=...".
var version = "dev"

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	if len(os.Args) < 2 {
		usage(os.Stderr)
		os.Exit(2)
	}

	cmd, args := os.Args[1], os.Args[2:]
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var err error
	switch cmd {
	case "api":
		err = runAPI(ctx, logger, args)
	case "migrate":
		err = runMigrate(ctx, logger, args)
	case "tokens":
		err = runTokens(ctx, logger, args)
	case "mcp":
		err = runMCP(ctx, logger, args)
	case "provider":
		err = runProvider(ctx, logger, args)
	case "healthcheck":
		err = runHealthcheck(ctx, logger, args)
	case "seed":
		err = runSeed(ctx, logger, args)
	case "node":
		err = runNode(ctx, logger, args)
	case "rebuild":
		err = runRebuild(ctx, logger, args)
	case "export":
		err = runExport(ctx, logger, args)
	case "export-context":
		err = runExportContext(ctx, logger, args)
	case "worker":
		err = runWorker(ctx, logger, args)
	case "spoke":
		err = runSpoke(ctx, logger, args)
	case "feed":
		err = runFeed(ctx, logger, args)
	case "safety":
		err = runSafety(ctx, logger, args)
	case "git":
		err = runGit(ctx, logger, args)
	case "version", "--version", "-v":
		fmt.Println(version)
	case "help", "--help", "-h":
		usage(os.Stdout)
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n\n", cmd)
		usage(os.Stderr)
		os.Exit(2)
	}

	if err != nil {
		// Cancellation by SIGINT/SIGTERM is the normal shutdown path; don't
		// noise it up as an error.
		if !errors.Is(err, context.Canceled) {
			logger.Error("command failed", slog.String("command", cmd), slog.String("error", err.Error()))
			os.Exit(1)
		}
	}
}

func runAPI(ctx context.Context, logger *slog.Logger, _ []string) error {
	pool, active, err := openProfileAwarePool(ctx, logger)
	if err != nil {
		return err
	}
	defer pool.Close()

	srv := api.NewWithPolicy(pool, logger, active.Policy)
	return srv.Run(ctx)
}

func runMigrate(ctx context.Context, logger *slog.Logger, args []string) error {
	direction := "up"
	if len(args) > 0 {
		direction = args[0]
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

	switch direction {
	case "up":
		return storage.Migrate(ctx, pool, logger)
	case "down":
		return storage.MigrateDown(ctx, pool, logger)
	default:
		return fmt.Errorf("migrate: unknown direction %q (want \"up\" or \"down\")", direction)
	}
}

func usage(w *os.File) {
	fmt.Fprintf(w, `meristem %s

usage:
  meristem api               run the HTTP API
  meristem migrate [up|down] apply or roll back database migrations
  meristem tokens ...        create, list, or revoke bearer tokens
  meristem mcp               run the MCP stdio server (reads MERISTEM_TOKEN)
  meristem seed v1           seed the v1 substrate backlog (reads MERISTEM_TOKEN, must be source=system)
  meristem node ...          register nodes and reconcile the registry (node sync-registry --once for one tick)
  meristem rebuild           fold events through projectors into a sandbox schema and diff vs live
  meristem worker            run the deterministic reconciler daemon (reads MERISTEM_TOKEN, must be source=system)
  meristem worker --once     run one deterministic reconciler tick
  meristem spoke             run the pull-only outbound poll loop (drains the hub command queue; reads MERISTEM_HUB_URL/NODE_ID/HUB_TOKEN/TOKEN)
  meristem feed [--watch]    human-readable terminal view of the activity log
  meristem export-context    deterministically materialize an allow/deny repo slice (operator-side; no API calls)
  meristem safety check      validate deterministic resource-safety controls
  meristem healthcheck       probe /readyz; exit 0 if healthy (for Docker HEALTHCHECK)
  meristem version           print version
  meristem help              show this message

environment:
  MERISTEM_DATABASE_URL    Postgres DSN (required for api, worker, migrate, mcp, tokens, seed)
  MERISTEM_HTTP_ADDR       listen address for the api (default :8080)
  MERISTEM_PUBLIC_BASE_URL external base URL for provider OAuth metadata (optional)
  MERISTEM_TOKEN           bearer secret for mcp (any token), tokens (root), seed/worker (system)
`, version)
}

func openProfileAwarePool(ctx context.Context, logger *slog.Logger) (*pgxpool.Pool, policyprofile.Active, error) {
	if _, _, err := validateStartupSafety(logger); err != nil {
		return nil, policyprofile.Active{}, err
	}

	cfg, err := storage.LoadConfigFromEnv()
	if err != nil {
		return nil, policyprofile.Active{}, err
	}

	bootstrapPool, err := storage.Open(ctx, cfg)
	if err != nil {
		return nil, policyprofile.Active{}, err
	}
	active, err := policyprofile.NewService(bootstrapPool, nil).Active(ctx)
	bootstrapPool.Close()
	if err != nil {
		return nil, policyprofile.Active{}, err
	}

	pool, err := storage.Open(ctx, cfg.WithPoolBounds(active.Policy.PoolMaxConns, active.Policy.PoolMinConns))
	if err != nil {
		return nil, policyprofile.Active{}, err
	}
	return pool, active, nil
}
