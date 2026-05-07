package cursorcli

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestRenderMCPConfigReferencesSecretsByPath(t *testing.T) {
	out, err := RenderMCPConfig(MCPConfigInput{
		MeristemRoot: "/tmp/meristem root",
		DatabaseURL:  "postgres://user:pass@localhost:5432/meristem?sslmode=disable",
		TokenFile:    ".meristem/worker.token",
		GoBin:        "/opt/homebrew/bin/go",
	})
	if err != nil {
		t.Fatalf("RenderMCPConfig: %v", err)
	}

	var cfg cursorMCPConfig
	if err := json.Unmarshal([]byte(out), &cfg); err != nil {
		t.Fatalf("config is not json: %v\n%s", err, out)
	}
	server, ok := cfg.MCPServers["meristem"]
	if !ok {
		t.Fatalf("missing meristem server: %s", out)
	}
	if server.Command != "bash" || len(server.Args) != 2 || server.Args[0] != "-lc" {
		t.Fatalf("unexpected command shape: %#v", server)
	}
	for _, want := range []string{
		"cd '/tmp/meristem root'",
		"MERISTEM_MCP_TOOL_NAMES=cursor",
		"$(tr -d '\\n' < '.meristem/worker.token')",
		"exec '/opt/homebrew/bin/go' run ./cmd/meristem mcp",
	} {
		if !strings.Contains(server.Args[1], want) {
			t.Fatalf("command missing %q:\n%s", want, server.Args[1])
		}
	}
	if strings.Contains(out, "mrs_") {
		t.Fatalf("config appears to contain a bearer token:\n%s", out)
	}
}

func TestRenderMCPConfigRequiresDatabaseURL(t *testing.T) {
	if _, err := RenderMCPConfig(MCPConfigInput{}); err == nil || !strings.Contains(err.Error(), "database url") {
		t.Fatalf("expected database url error, got %v", err)
	}
}

func TestBuildLaunchCommand(t *testing.T) {
	bin, args, err := BuildLaunchCommand(LaunchInput{
		CursorBin:     "/usr/local/bin/cursor-agent",
		Model:         "composer2",
		WorkspaceRoot: "/tmp/project",
		WorktreeName:  "wi-123",
		WorktreeBase:  "v1",
		Mode:          LaunchModePrint,
		Trust:         true,
		ApproveMCPs:   true,
		Prompt:        "do the narrow thing",
	})
	if err != nil {
		t.Fatalf("BuildLaunchCommand: %v", err)
	}
	if bin != "/usr/local/bin/cursor-agent" {
		t.Fatalf("bin = %q", bin)
	}
	joined := strings.Join(args, "\x00")
	for _, want := range []string{
		"--model\x00composer2",
		"--workspace\x00/tmp/project",
		"--worktree\x00wi-123",
		"--worktree-base\x00v1",
		"--print",
		"--trust",
		"--approve-mcps",
		"do the narrow thing",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("args missing %q: %#v", want, args)
		}
	}
}

func TestBuildLaunchCommandValidation(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   LaunchInput
		want string
	}{
		{
			name: "missing workspace",
			in:   LaunchInput{Prompt: "do it"},
			want: "workspace root",
		},
		{
			name: "missing prompt",
			in:   LaunchInput{WorkspaceRoot: "/tmp/project"},
			want: "prompt",
		},
		{
			name: "unsupported mode",
			in:   LaunchInput{WorkspaceRoot: "/tmp/project", Prompt: "do it", Mode: "yolo"},
			want: "unsupported launch mode",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := BuildLaunchCommand(tc.in)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected %q error, got %v", tc.want, err)
			}
		})
	}
}
