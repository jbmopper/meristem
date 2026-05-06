package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/providers/cursorcli"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/workitems"
)

func runProvider(ctx context.Context, _ *slog.Logger, args []string) error {
	if len(args) < 2 {
		providerUsage(os.Stderr)
		return fmt.Errorf("provider: missing provider and action")
	}
	switch args[0] {
	case "cursor-cli":
		return runCursorCLIProvider(ctx, args[1:])
	default:
		providerUsage(os.Stderr)
		return fmt.Errorf("provider: unknown provider %q", args[0])
	}
}

func runCursorCLIProvider(ctx context.Context, args []string) error {
	if len(args) == 0 {
		cursorCLIProviderUsage(os.Stderr)
		return fmt.Errorf("provider cursor-cli: missing action")
	}
	switch args[0] {
	case "scaffold":
		return runCursorCLIScaffold(ctx, args[1:])
	default:
		cursorCLIProviderUsage(os.Stderr)
		return fmt.Errorf("provider cursor-cli: unknown action %q", args[0])
	}
}

func runCursorCLIScaffold(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("provider cursor-cli scaffold", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	workItemRaw := fs.String("work-item", "", "work_item UUID to hand off")
	scope := fs.String("scope", "", "scope sentence for the worker")
	model := fs.String("model", cursorcli.DefaultModel, "Cursor model label for the handoff")
	tokenFile := fs.String("token-file", cursorcli.DefaultTokenFile, "agent token file path to reference, not read")
	repoRoot := fs.String("repo-root", cursorcli.DefaultRepoRoot, "repo root path to include in setup instructions")
	var allowed repeatFlag
	var outOfScope repeatFlag
	fs.Var(&allowed, "allowed-area", "allowed path/module/system; repeatable")
	fs.Var(&outOfScope, "out-of-scope", "explicitly forbidden area or behavior; repeatable")
	if err := fs.Parse(args); err != nil {
		cursorCLIScaffoldUsage(os.Stderr)
		return err
	}

	id, err := uuid.Parse(strings.TrimSpace(*workItemRaw))
	if err != nil {
		cursorCLIScaffoldUsage(os.Stderr)
		return fmt.Errorf("provider cursor-cli scaffold: --work-item must be a valid uuid")
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

	item, err := workitems.NewService(pool, nil).Get(ctx, id)
	if err != nil {
		return err
	}

	scaffold, err := cursorcli.RenderScaffold(cursorcli.ScaffoldInput{
		WorkItem:     item,
		Scope:        *scope,
		AllowedAreas: allowed,
		OutOfScope:   outOfScope,
		Model:        *model,
		TokenFile:    *tokenFile,
		RepoRoot:     *repoRoot,
	})
	if err != nil {
		return err
	}
	fmt.Fprint(os.Stdout, scaffold)
	if !strings.HasSuffix(scaffold, "\n") {
		fmt.Fprintln(os.Stdout)
	}
	return nil
}

type repeatFlag []string

func (f *repeatFlag) String() string {
	if f == nil {
		return ""
	}
	return strings.Join(*f, ",")
}

func (f *repeatFlag) Set(value string) error {
	*f = append(*f, value)
	return nil
}

func providerUsage(w io.Writer) {
	fmt.Fprint(w, `usage:
  meristem provider cursor-cli scaffold --work-item UUID --scope TEXT --allowed-area PATH [...]

Provider helpers produce handoff scaffolding for external workers. They do
not create durable provider identity in the schema; attribution remains the
agent-source token used by each worker.
`)
}

func cursorCLIProviderUsage(w io.Writer) {
	fmt.Fprint(w, `usage:
  meristem provider cursor-cli scaffold --work-item UUID --scope TEXT --allowed-area PATH [...]

Actions:
  scaffold  Print a secret-free Cursor CLI worker handoff packet.
`)
}

func cursorCLIScaffoldUsage(w io.Writer) {
	fmt.Fprint(w, `usage:
  meristem provider cursor-cli scaffold \
    --work-item UUID \
    --scope TEXT \
    --allowed-area PATH_OR_MODULE [--allowed-area PATH_OR_MODULE ...] \
    [--out-of-scope TEXT ...] \
    [--model composer2] \
    [--token-file .meristem/cursor-cli.token] \
    [--repo-root .]

Reads the work_item projection from Postgres and prints a handoff packet
with suggested convergence checks, human review status, MCP setup, a worker
prompt, and an AGENTS.md overlay. Token files are referenced by path only.
`)
}
