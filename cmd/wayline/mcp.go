package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/jbmopper/wayline/internal/app"
	"github.com/jbmopper/wayline/internal/auth"
	"github.com/jbmopper/wayline/internal/feed"
	"github.com/jbmopper/wayline/internal/inbox"
	"github.com/jbmopper/wayline/internal/mcp"
	"github.com/jbmopper/wayline/internal/storage"
	"github.com/jbmopper/wayline/internal/workitems"
)

// runMCP launches the MCP stdio server. The bearer token is read from
// WAYLINE_TOKEN at process start; per docs/v0.md every MCP-connected
// agent (each Cursor instance, each custom worker) gets its own token row,
// so the token-per-process model matches how Cursor launches MCP servers.
//
// Stdout is reserved for JSON-RPC traffic; structured logs go to stderr
// so launching clients can pipe stdio cleanly.
func runMCP(ctx context.Context, logger *slog.Logger, _ []string) error {
	secret := os.Getenv("WAYLINE_TOKEN")
	if secret == "" {
		return fmt.Errorf("mcp: WAYLINE_TOKEN is required (mint one with `wayline tokens create --source agent`)")
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

	writer := app.NewEventWriter()
	deps := mcp.Deps{
		Auth:      auth.NewService(pool, writer),
		Inbox:     inbox.NewService(pool, writer),
		WorkItems: workitems.NewService(pool, writer),
		Feed:      feed.NewService(pool),
	}

	server := mcp.New(deps, mcp.ServerInfo{Name: "wayline", Version: version}, logger)
	if err := server.Authenticate(ctx, secret); err != nil {
		return fmt.Errorf("mcp: authenticate WAYLINE_TOKEN: %w", err)
	}

	logger.Info("mcp server ready",
		slog.String("transport", "stdio"),
		slog.String("version", version),
	)

	return server.Run(ctx, os.Stdin, os.Stdout)
}
