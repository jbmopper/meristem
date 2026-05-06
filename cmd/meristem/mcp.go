package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/feed"
	"github.com/jbmopper/meristem/internal/inbox"
	"github.com/jbmopper/meristem/internal/mcp"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/workitems"
)

// runMCP launches the MCP stdio server. The bearer token is read from
// MERISTEM_TOKEN at process start; per docs/v0.md every MCP-connected
// agent (each Cursor instance, each custom worker) gets its own token row,
// so the token-per-process model matches how Cursor launches MCP servers.
//
// Stdout is reserved for JSON-RPC traffic; structured logs go to stderr
// so launching clients can pipe stdio cleanly.
func runMCP(ctx context.Context, logger *slog.Logger, _ []string) error {
	policy, _, err := validateStartupSafety(logger)
	if err != nil {
		return err
	}

	secret := os.Getenv("MERISTEM_TOKEN")
	if secret == "" {
		return fmt.Errorf("mcp: MERISTEM_TOKEN is required (mint one with `meristem tokens create --source agent`)")
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
		Auth:        auth.NewService(pool, writer),
		Inbox:       inbox.NewService(pool, writer),
		WorkItems:   workitems.NewService(pool, writer),
		Feed:        feed.NewService(pool),
		MaxFeedWait: policy.MaxFeedWait,
	}

	server := mcp.New(deps, mcp.ServerInfo{Name: "meristem", Version: version}, logger)
	if os.Getenv("MERISTEM_MCP_TOOL_NAMES") == string(mcp.ToolNameModeCursor) {
		server.SetToolNameMode(mcp.ToolNameModeCursor)
	}
	if err := server.Authenticate(ctx, secret); err != nil {
		return fmt.Errorf("mcp: authenticate MERISTEM_TOKEN: %w", err)
	}

	logger.Info("mcp server ready",
		slog.String("transport", "stdio"),
		slog.String("version", version),
	)

	return server.Run(ctx, os.Stdin, os.Stdout)
}
