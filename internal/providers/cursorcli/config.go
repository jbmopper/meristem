package cursorcli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

const (
	DefaultCursorBin      = "cursor-agent"
	LaunchModeInteractive = "interactive"
	LaunchModePrint       = "print"
)

// MCPConfigInput describes a Cursor MCP config that can live in any target
// workspace while still launching meristem's stdio MCP server from this repo.
type MCPConfigInput struct {
	MeristemRoot string
	DatabaseURL  string
	TokenFile    string
	GoBin        string
}

// RenderMCPConfig returns a secret-free .cursor/mcp.json body.
func RenderMCPConfig(in MCPConfigInput) (string, error) {
	in = normalizeMCPConfig(in)
	if in.DatabaseURL == "" {
		return "", fmt.Errorf("cursorcli: database url is required")
	}
	command := fmt.Sprintf(
		"cd %s && MERISTEM_DATABASE_URL=%s MERISTEM_MCP_TOOL_NAMES=cursor MERISTEM_TOKEN=$(tr -d '\\n' < %s) exec %s run ./cmd/meristem mcp",
		shellQuote(in.MeristemRoot),
		shellQuote(in.DatabaseURL),
		shellQuote(in.TokenFile),
		shellQuote(in.GoBin),
	)
	cfg := cursorMCPConfig{
		MCPServers: map[string]cursorMCPServer{
			"meristem": {
				Command: "bash",
				Args:    []string{"-lc", command},
			},
		},
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(cfg); err != nil {
		return "", err
	}
	return buf.String(), nil
}

type cursorMCPConfig struct {
	MCPServers map[string]cursorMCPServer `json:"mcpServers"`
}

type cursorMCPServer struct {
	Command string   `json:"command"`
	Args    []string `json:"args"`
}

func normalizeMCPConfig(in MCPConfigInput) MCPConfigInput {
	in.MeristemRoot = strings.TrimSpace(in.MeristemRoot)
	if in.MeristemRoot == "" {
		in.MeristemRoot = DefaultMeristemRoot
	}
	in.TokenFile = strings.TrimSpace(in.TokenFile)
	if in.TokenFile == "" {
		in.TokenFile = DefaultMCPTokenFile
	}
	in.GoBin = strings.TrimSpace(in.GoBin)
	if in.GoBin == "" {
		in.GoBin = "go"
	}
	in.DatabaseURL = strings.TrimSpace(in.DatabaseURL)
	return in
}

// LaunchInput describes a Cursor Agent invocation.
type LaunchInput struct {
	CursorBin     string
	Model         string
	WorkspaceRoot string
	WorktreeName  string
	WorktreeBase  string
	Mode          string
	Trust         bool
	ApproveMCPs   bool
	Prompt        string
}

// BuildLaunchCommand converts launch intent into an argv vector. It never
// shells out, so prompts are passed as one argument and secrets are not expanded.
func BuildLaunchCommand(in LaunchInput) (string, []string, error) {
	in = normalizeLaunch(in)
	if in.WorkspaceRoot == "" {
		return "", nil, fmt.Errorf("cursorcli: workspace root is required")
	}
	if strings.TrimSpace(in.Prompt) == "" {
		return "", nil, fmt.Errorf("cursorcli: prompt is required")
	}

	args := []string{
		"--model", in.Model,
		"--workspace", in.WorkspaceRoot,
	}
	if in.WorktreeName != "" {
		args = append(args, "--worktree", in.WorktreeName)
	}
	if in.WorktreeBase != "" {
		args = append(args, "--worktree-base", in.WorktreeBase)
	}
	switch in.Mode {
	case LaunchModeInteractive:
	case LaunchModePrint:
		args = append(args, "--print")
		if in.Trust {
			args = append(args, "--trust")
		}
		if in.ApproveMCPs {
			args = append(args, "--approve-mcps")
		}
	default:
		return "", nil, fmt.Errorf("cursorcli: unsupported launch mode %q", in.Mode)
	}
	args = append(args, in.Prompt)
	return in.CursorBin, args, nil
}

func normalizeLaunch(in LaunchInput) LaunchInput {
	in.CursorBin = strings.TrimSpace(in.CursorBin)
	if in.CursorBin == "" {
		in.CursorBin = DefaultCursorBin
	}
	in.Model = strings.TrimSpace(in.Model)
	if in.Model == "" {
		in.Model = DefaultModel
	} else {
		in.Model = NormalizeModel(in.Model)
	}
	in.WorkspaceRoot = strings.TrimSpace(in.WorkspaceRoot)
	in.WorktreeName = strings.TrimSpace(in.WorktreeName)
	in.WorktreeBase = strings.TrimSpace(in.WorktreeBase)
	in.Mode = strings.TrimSpace(in.Mode)
	if in.Mode == "" {
		in.Mode = LaunchModeInteractive
	}
	in.Prompt = strings.TrimSpace(in.Prompt)
	return in
}
