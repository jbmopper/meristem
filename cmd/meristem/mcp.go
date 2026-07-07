package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/approvals"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/convergence"
	"github.com/jbmopper/meristem/internal/cultivaractivation"
	"github.com/jbmopper/meristem/internal/errorreporting"
	"github.com/jbmopper/meristem/internal/feed"
	"github.com/jbmopper/meristem/internal/httpconnector"
	"github.com/jbmopper/meristem/internal/idempotency"
	"github.com/jbmopper/meristem/internal/inbox"
	"github.com/jbmopper/meristem/internal/mcp"
	"github.com/jbmopper/meristem/internal/policyprofile"
	"github.com/jbmopper/meristem/internal/projectiondefs"
	"github.com/jbmopper/meristem/internal/registry"
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
	secret := os.Getenv("MERISTEM_TOKEN")
	if secret == "" {
		return fmt.Errorf("mcp: MERISTEM_TOKEN is required (mint one with `meristem tokens create --source agent`)")
	}

	pool, active, err := openProfileAwarePool(ctx, logger)
	if err != nil {
		return err
	}
	defer pool.Close()

	writer := app.NewEventWriter()
	approvalSvc := approvals.NewService(pool, writer)
	// Wire every service the shared mcp.Deps advertises tools for, matching
	// internal/api/server.go. Historically this stdio launcher drifted behind
	// the HTTP path as services were added, so registry/approval/activation/
	// proposal/connector/projection tools were advertised but answered
	// "…not configured" over stdio.
	deps := mcp.Deps{
		Auth:                auth.NewService(pool, writer),
		Access:              access.NewService(pool),
		Idempotency:         idempotency.NewMiddleware(pool, writer),
		Inbox:               inbox.NewService(pool, writer),
		WorkItems:           workitems.NewService(pool, writer),
		Approvals:           approvalSvc,
		HTTPConnector:       httpconnector.NewService(pool, writer, approvalSvc, nil),
		CheckProposals:      convergence.NewChecksProposalService(pool, writer),
		CultivarActivations: cultivaractivation.NewService(pool, writer),
		DeterministicErrors: errorreporting.NewService(pool, writer),
		Feed:                feed.NewService(pool),
		PolicyProfiles:      policyprofile.NewService(pool, writer),
		Projections:         projectiondefs.NewService(pool, writer),
		Registry:            registry.NewService(pool, writer),
		MaxFeedWait:         active.Policy.MaxFeedWait,
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
