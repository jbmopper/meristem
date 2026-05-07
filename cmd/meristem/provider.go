package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
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
	case "mcp-config":
		return runCursorCLIMCPConfig(args[1:])
	case "launch":
		return runCursorCLILaunch(ctx, args[1:])
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
	model := fs.String("model", cursorcli.DefaultModel, "Cursor model label for the handoff; aliases: spark, 5.3-spark")
	tokenFile := fs.String("token-file", cursorcli.DefaultTokenFile, "agent token file path to reference, not read")
	meristemRoot := fs.String("meristem-root", defaultMeristemRoot(), "meristem repo root path for MCP setup")
	repoRoot := fs.String("repo-root", "", "deprecated alias for --meristem-root")
	workspaceRoot := fs.String("workspace-root", cursorcli.DefaultWorkspaceRoot, "target workspace path where the worker edits")
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
		WorkItem:      item,
		Scope:         *scope,
		AllowedAreas:  allowed,
		OutOfScope:    outOfScope,
		Model:         *model,
		TokenFile:     *tokenFile,
		RepoRoot:      *repoRoot,
		MeristemRoot:  *meristemRoot,
		WorkspaceRoot: *workspaceRoot,
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

func runCursorCLIMCPConfig(args []string) error {
	fs := flag.NewFlagSet("provider cursor-cli mcp-config", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	workspaceRoot := fs.String("workspace", cursorcli.DefaultWorkspaceRoot, "target workspace where .cursor/mcp.json lives")
	meristemRoot := fs.String("meristem-root", defaultMeristemRoot(), "meristem repo root path to run the MCP server from")
	tokenFile := fs.String("token-file", cursorcli.DefaultMCPTokenFile, "agent token file path to reference, not read")
	goBin := fs.String("go-bin", defaultGoBin(), "go binary used to run meristem mcp")
	databaseURL := fs.String("database-url", os.Getenv(storage.EnvDatabaseURL), "Postgres DSN to bake into the MCP command")
	apply := fs.Bool("apply", false, "write .cursor/mcp.json in the target workspace instead of printing")
	force := fs.Bool("force", false, "allow replacing an existing .cursor/mcp.json")
	if err := fs.Parse(args); err != nil {
		cursorCLIMCPConfigUsage(os.Stderr)
		return err
	}
	body, err := cursorcli.RenderMCPConfig(cursorcli.MCPConfigInput{
		MeristemRoot: *meristemRoot,
		DatabaseURL:  *databaseURL,
		TokenFile:    *tokenFile,
		GoBin:        *goBin,
	})
	if err != nil {
		return err
	}
	if !*apply {
		fmt.Fprint(os.Stdout, body)
		return nil
	}
	path, err := writeCursorMCPConfig(*workspaceRoot, body, *force)
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "wrote %s\n", path)
	return nil
}

func runCursorCLILaunch(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("provider cursor-cli launch", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	workItemRaw := fs.String("work-item", "", "work_item UUID to hand off")
	scope := fs.String("scope", "", "scope sentence for the worker")
	workspaceRoot := fs.String("workspace", "", "target workspace path where Cursor Agent edits")
	meristemRoot := fs.String("meristem-root", defaultMeristemRoot(), "meristem repo root path for MCP setup")
	model := fs.String("model", cursorcli.DefaultModel, "Cursor model label; aliases: spark, 5.3-spark")
	cursorBin := fs.String("cursor-bin", cursorcli.DefaultCursorBin, "cursor-agent binary")
	tokenFile := fs.String("token-file", cursorcli.DefaultTokenFile, "agent token file path to reference, not read")
	goBin := fs.String("go-bin", defaultGoBin(), "go binary used by generated MCP config")
	worktreeName := fs.String("worktree", "", "optional Cursor worktree name")
	worktreeBase := fs.String("worktree-base", "", "optional Cursor worktree base branch/ref in the target workspace")
	mode := fs.String("mode", cursorcli.LaunchModeInteractive, "launch mode: interactive or print")
	applyMCP := fs.Bool("apply-mcp", false, "write .cursor/mcp.json into the target workspace before launch")
	forceMCP := fs.Bool("force-mcp", false, "allow replacing an existing target .cursor/mcp.json")
	approveMCPs := fs.Bool("approve-mcps", false, "pass --approve-mcps to Cursor Agent in print mode")
	trust := fs.Bool("trust", false, "pass --trust to Cursor Agent in print mode")
	dryRun := fs.Bool("dry-run", false, "print the Cursor Agent argv and prompt without launching")
	var allowed repeatFlag
	var outOfScope repeatFlag
	fs.Var(&allowed, "allowed-area", "allowed path/module/system; repeatable")
	fs.Var(&outOfScope, "out-of-scope", "explicitly forbidden area or behavior; repeatable")
	if err := fs.Parse(args); err != nil {
		cursorCLILaunchUsage(os.Stderr)
		return err
	}

	id, err := uuid.Parse(strings.TrimSpace(*workItemRaw))
	if err != nil {
		cursorCLILaunchUsage(os.Stderr)
		return fmt.Errorf("provider cursor-cli launch: --work-item must be a valid uuid")
	}
	if strings.TrimSpace(*workspaceRoot) == "" {
		cursorCLILaunchUsage(os.Stderr)
		return fmt.Errorf("provider cursor-cli launch: --workspace is required")
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
	prompt, err := cursorcli.RenderScaffold(cursorcli.ScaffoldInput{
		WorkItem:      item,
		Scope:         *scope,
		AllowedAreas:  allowed,
		OutOfScope:    outOfScope,
		Model:         *model,
		TokenFile:     *tokenFile,
		MeristemRoot:  *meristemRoot,
		WorkspaceRoot: *workspaceRoot,
	})
	if err != nil {
		return err
	}

	if *applyMCP {
		body, err := cursorcli.RenderMCPConfig(cursorcli.MCPConfigInput{
			MeristemRoot: *meristemRoot,
			DatabaseURL:  cfg.DatabaseURL,
			TokenFile:    *tokenFile,
			GoBin:        *goBin,
		})
		if err != nil {
			return err
		}
		path, err := writeCursorMCPConfig(*workspaceRoot, body, *forceMCP)
		if err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "wrote %s\n", path)
	}

	bin, launchArgs, err := cursorcli.BuildLaunchCommand(cursorcli.LaunchInput{
		CursorBin:     *cursorBin,
		Model:         *model,
		WorkspaceRoot: *workspaceRoot,
		WorktreeName:  *worktreeName,
		WorktreeBase:  *worktreeBase,
		Mode:          *mode,
		Trust:         *trust,
		ApproveMCPs:   *approveMCPs,
		Prompt:        prompt,
	})
	if err != nil {
		return err
	}
	if *dryRun {
		fmt.Fprintf(os.Stdout, "%s\n\nPrompt:\n%s", shellJoin(append([]string{bin}, launchArgs[:len(launchArgs)-1]...)), prompt)
		return nil
	}
	cmd := exec.CommandContext(ctx, bin, launchArgs...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func writeCursorMCPConfig(workspaceRoot, body string, force bool) (string, error) {
	workspaceRoot = strings.TrimSpace(workspaceRoot)
	if workspaceRoot == "" {
		return "", fmt.Errorf("provider cursor-cli mcp-config: --workspace is required when --apply is set")
	}
	dir := filepath.Join(workspaceRoot, ".cursor")
	path := filepath.Join(dir, "mcp.json")
	if !force {
		if _, err := os.Stat(path); err == nil {
			return "", fmt.Errorf("provider cursor-cli mcp-config: %s already exists; pass --force to replace it", path)
		} else if !os.IsNotExist(err) {
			return "", err
		}
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		return "", err
	}
	return path, nil
}

func defaultGoBin() string {
	if path, err := exec.LookPath("go"); err == nil {
		return path
	}
	return "go"
}

func defaultMeristemRoot() string {
	if wd, err := os.Getwd(); err == nil {
		if abs, err := filepath.Abs(wd); err == nil {
			return abs
		}
		return wd
	}
	return cursorcli.DefaultMeristemRoot
}

func shellJoin(args []string) string {
	quoted := make([]string, 0, len(args))
	for _, arg := range args {
		quoted = append(quoted, "'"+strings.ReplaceAll(arg, "'", `'"'"'`)+"'")
	}
	return strings.Join(quoted, " ")
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
  meristem provider cursor-cli mcp-config --workspace PATH [--apply]
  meristem provider cursor-cli launch --work-item UUID --workspace PATH --scope TEXT --allowed-area PATH [...]

Actions:
  scaffold    Print a secret-free Cursor CLI worker handoff packet.
  mcp-config  Print or apply target-workspace Cursor MCP config for meristem.
  launch      Launch cursor-agent with a generated meristem handoff prompt.
`)
}

func cursorCLIScaffoldUsage(w io.Writer) {
	fmt.Fprint(w, `usage:
  meristem provider cursor-cli scaffold \
    --work-item UUID \
    --scope TEXT \
    --allowed-area PATH_OR_MODULE [--allowed-area PATH_OR_MODULE ...] \
    [--out-of-scope TEXT ...] \
    [--model composer-2|spark|5.3-spark] \
    [--token-file .meristem/cursor-cli.token] \
    [--meristem-root .] \
    [--workspace-root .]

Reads the work_item projection from Postgres and prints a handoff packet
with suggested convergence checks, human review status, MCP setup, a worker
prompt, and an AGENTS.md overlay. Token files are referenced by path only.
`)
}

func cursorCLIMCPConfigUsage(w io.Writer) {
	fmt.Fprint(w, `usage:
  meristem provider cursor-cli mcp-config \
    --workspace PATH \
    [--apply] [--force] \
    [--meristem-root .] \
    [--token-file .meristem/cursor-cli.token] \
    [--go-bin go]

Without --apply, prints a secret-free .cursor/mcp.json body. With --apply,
writes it into the target workspace and refuses to replace an existing file
unless --force is set.
`)
}

func cursorCLILaunchUsage(w io.Writer) {
	fmt.Fprint(w, `usage:
  meristem provider cursor-cli launch \
    --work-item UUID \
    --workspace PATH \
    --scope TEXT \
    --allowed-area PATH_OR_MODULE [--allowed-area PATH_OR_MODULE ...] \
    [--apply-mcp] [--force-mcp] \
    [--worktree NAME] [--worktree-base TARGET_REF] \
    [--model composer-2|spark|5.3-spark] \
    [--mode interactive|print] \
    [--approve-mcps] [--trust] \
    [--dry-run]

Generates a live meristem handoff prompt and invokes cursor-agent against the
target workspace. --worktree-base is a ref in that target workspace, not
necessarily a meristem ref. Use --dry-run to inspect the exact argv and prompt
first.
`)
}
