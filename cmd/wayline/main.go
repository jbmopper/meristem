// wayline is the single binary for the wayline coordination plane.
//
// Two runtime modes share one codebase and one database, per the spec:
//   wayline api               - HTTP surface
//   wayline worker --once     - one-shot bounded-patience scan (v1 kernel)
//
// Plus operational subcommands:
//   wayline migrate  - apply pending Postgres migrations
//   wayline tokens   - mint, list, and revoke bearer tokens
//   wayline mcp      - run the MCP stdio server
//   wayline seed     - seed substrate backlogs into the running system
//   wayline rebuild  - rebuild projections from events into a sandbox schema and diff
//   wayline version  - print build info
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/jbmopper/wayline/internal/api"
	"github.com/jbmopper/wayline/internal/storage"
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
	case "healthcheck":
		err = runHealthcheck(ctx, logger, args)
	case "seed":
		err = runSeed(ctx, logger, args)
	case "rebuild":
		err = runRebuild(ctx, logger, args)
	case "worker":
		err = runWorker(ctx, logger, args)
	case "feed":
		err = runFeed(ctx, logger, args)
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
	cfg, err := storage.LoadConfigFromEnv()
	if err != nil {
		return err
	}
	pool, err := storage.Open(ctx, cfg)
	if err != nil {
		return err
	}
	defer pool.Close()

	srv := api.New(pool, logger)
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
	fmt.Fprintf(w, `wayline %s

usage:
  wayline api               run the HTTP API
  wayline migrate [up|down] apply or roll back database migrations
  wayline tokens ...        create, list, or revoke bearer tokens
  wayline mcp               run the MCP stdio server (reads WAYLINE_TOKEN)
  wayline seed v1           seed the v1 substrate backlog (reads WAYLINE_TOKEN, must be source=system)
  wayline rebuild           fold events through projectors into a sandbox schema and diff vs live
  wayline worker --once     one-shot bounded-patience scan (reads WAYLINE_TOKEN, must be source=system)
  wayline feed [--watch]    human-readable terminal view of the activity log
  wayline healthcheck       probe /readyz; exit 0 if healthy (for Docker HEALTHCHECK)
  wayline version           print version
  wayline help              show this message

environment:
  WAYLINE_DATABASE_URL  Postgres DSN (required for api, migrate, mcp, tokens, seed)
  WAYLINE_HTTP_ADDR     listen address for the api (default :8080)
  WAYLINE_TOKEN         bearer secret for mcp (any token), tokens (root), seed (system)
`, version)
}
