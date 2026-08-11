package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/approvals"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/buildguard"
	"github.com/jbmopper/meristem/internal/convergence"
	"github.com/jbmopper/meristem/internal/cultivaractivation"
	"github.com/jbmopper/meristem/internal/errorreporting"
	"github.com/jbmopper/meristem/internal/feed"
	"github.com/jbmopper/meristem/internal/httpconnector"
	"github.com/jbmopper/meristem/internal/idempotency"
	"github.com/jbmopper/meristem/internal/inbox"
	"github.com/jbmopper/meristem/internal/listeneractivation"
	"github.com/jbmopper/meristem/internal/listeners"
	"github.com/jbmopper/meristem/internal/mcp"
	"github.com/jbmopper/meristem/internal/oauth"
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
func runMCP(ctx context.Context, logger *slog.Logger, _ []string, build buildguard.StatusProvider) error {
	secret := os.Getenv("MERISTEM_TOKEN")
	if secret == "" {
		return fmt.Errorf("mcp: MERISTEM_TOKEN is required (mint one with `meristem tokens create --source agent`)")
	}
	taskBinding, err := listenerTaskMCPBindingFromEnv()
	if err != nil {
		return err
	}

	pool, active, err := openProfileAwarePool(ctx, logger)
	if err != nil {
		return err
	}
	defer pool.Close()

	writer := app.NewGuardedEventWriter(build)
	approvalSvc := approvals.NewService(pool, writer)
	// Wire every service the shared mcp.Deps advertises tools for, matching
	// internal/api/server.go. Historically this stdio launcher drifted behind
	// the HTTP path as services were added, so registry/approval/activation/
	// proposal/connector/projection tools were advertised but answered
	// "…not configured" over stdio.
	deps := mcp.Deps{
		Auth:   auth.NewService(pool, writer),
		Access: access.NewService(pool),
		Idempotency: idempotency.NewMiddlewareWithGuard(pool, writer, func() error {
			return buildguard.RequireNonBlocking(build)
		}),
		Inbox:               inbox.NewService(pool, writer),
		OAuthClientAdmin:    oauth.NewClientAdminService(pool, writer),
		WorkItems:           workitems.NewService(pool, writer),
		Listeners:           listeners.NewService(pool, writer),
		ListenerActivations: listeneractivation.NewService(pool, writer),
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

	server := mcp.New(deps, mcp.ServerInfo{Name: "meristem", Version: version, BuildStatus: build}, logger)
	if os.Getenv("MERISTEM_MCP_TOOL_NAMES") == string(mcp.ToolNameModeCursor) {
		server.SetToolNameMode(mcp.ToolNameModeCursor)
	}
	if taskBinding == nil {
		if err := server.Authenticate(ctx, secret); err != nil {
			return fmt.Errorf("mcp: authenticate MERISTEM_TOKEN: %w", err)
		}
	} else if err := server.AuthenticateListenerTask(ctx, secret, *taskBinding); err != nil {
		return fmt.Errorf("mcp: authenticate assignment-bound listener task: %w", err)
	}

	buildStatus := build.Status()
	logger.Info("mcp server ready",
		slog.String("transport", "stdio"),
		slog.String("version", buildStatus.Version()),
		slog.String("build_state", string(buildStatus.State)),
	)

	return server.Run(ctx, os.Stdin, os.Stdout)
}

func listenerTaskMCPBindingFromEnv() (*mcp.ListenerTaskBinding, error) {
	values := map[string]string{
		"expected actor":   os.Getenv("MERISTEM_MCP_EXPECT_ACTOR_ID"),
		"activation":       os.Getenv("MERISTEM_MCP_LISTENER_ACTIVATION_ID"),
		"work item":        os.Getenv("MERISTEM_MCP_LISTENER_WORK_ITEM_ID"),
		"assignment event": os.Getenv("MERISTEM_MCP_LISTENER_ASSIGNMENT_EVENT_ID"),
	}
	present := 0
	for _, value := range values {
		if value != "" {
			present++
		}
	}
	if present == 0 {
		return nil, nil
	}
	if present != len(values) {
		return nil, fmt.Errorf("mcp: listener task binding variables must be all present or all absent")
	}
	parse := func(name, value string) (uuid.UUID, error) {
		id, err := uuid.Parse(value)
		if err != nil || id == uuid.Nil || value != id.String() || value != strings.TrimSpace(value) {
			return uuid.Nil, fmt.Errorf("mcp: %s must be one canonical non-nil uuid", name)
		}
		return id, nil
	}
	expected, err := parse("expected actor", values["expected actor"])
	if err != nil {
		return nil, err
	}
	activationID, err := parse("activation", values["activation"])
	if err != nil {
		return nil, err
	}
	workItemID, err := parse("work item", values["work item"])
	if err != nil {
		return nil, err
	}
	assignmentID, err := parse("assignment event", values["assignment event"])
	if err != nil {
		return nil, err
	}
	return &mcp.ListenerTaskBinding{
		ActivationID: activationID, WorkItemID: workItemID,
		AssignmentEventID: assignmentID, ExpectedActorID: expected,
	}, nil
}
