package buildguard

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestLauncherPreflight(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("launcher is a bash script")
	}
	helper := filepath.Join("..", "..", "scripts", "check-meristem-build-pin.sh")
	tests := []struct {
		name          string
		binaryCommit  string
		binarySuffix  string
		pin           string
		wantOK        bool
		forbiddenText string
	}{
		{name: "current canonical", binaryCommit: commitA, pin: commitA + "\n", wantOK: true},
		{name: "current without newline", binaryCommit: commitA, pin: commitA, wantOK: true},
		{name: "stale", binaryCommit: commitA, pin: commitB + "\n"},
		{name: "uppercase", binaryCommit: commitA, pin: strings.ToUpper(commitA) + "\n"},
		{name: "extra newline", binaryCommit: commitA, pin: commitA + "\n\n"},
		{name: "invalid binary fingerprint", binaryCommit: "dev", pin: commitA + "\n"},
		{name: "extra binary newline", binaryCommit: commitA, binarySuffix: "\\n", pin: commitA + "\n"},
		{name: "malformed content stays secret", binaryCommit: commitA, pin: "sensitive malformed pin\n", forbiddenText: "sensitive malformed pin"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			binary := filepath.Join(dir, "meristem-bin")
			pin := binary + ".v1-pin"
			script := "#!/usr/bin/env bash\nif [[ \"${1:-}\" == build-guard-status && \"$#\" -eq 1 ]]; then printf 'meristem-build-guard-v1 %s\\n" + tc.binarySuffix + "' '" + tc.binaryCommit + "'; exit 0; fi\nexit 99\n"
			if err := os.WriteFile(binary, []byte(script), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(pin, []byte(tc.pin), 0o600); err != nil {
				t.Fatal(err)
			}
			output, err := exec.Command("bash", helper, binary, pin).CombinedOutput()
			if tc.wantOK && err != nil {
				t.Fatalf("preflight failed: %v: %s", err, output)
			}
			if !tc.wantOK && err == nil {
				t.Fatal("preflight unexpectedly succeeded")
			}
			text := string(output)
			if strings.Contains(text, binary) || strings.Contains(text, pin) || (tc.forbiddenText != "" && strings.Contains(text, tc.forbiddenText)) {
				t.Fatalf("preflight exposed path or pin content: %q", text)
			}
		})
	}
}

func TestLauncherPreflightRejectsPreGuardArgIgnoringBinary(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("launcher is a bash script")
	}
	dir := t.TempDir()
	binary := filepath.Join(dir, "meristem-bin")
	pin := binary + ".v1-pin"
	// This mirrors the historical CLI: `version` printed the stamped release
	// label while silently ignoring every trailing argument, but the dedicated
	// guard capability command did not exist.
	oldBinary := "#!/usr/bin/env bash\nif [[ \"${1:-}\" == version ]]; then printf '%s\\n' '" + commitA + "'; exit 0; fi\nexit 2\n"
	if err := os.WriteFile(binary, []byte(oldBinary), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(pin, []byte(commitA+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	helper := filepath.Join("..", "..", "scripts", "check-meristem-build-pin.sh")
	output, err := exec.Command("bash", helper, binary, pin).CombinedOutput()
	if err == nil {
		t.Fatalf("pre-guard binary unexpectedly passed launcher check: %s", output)
	}
	if strings.Contains(string(output), binary) || strings.Contains(string(output), pin) {
		t.Fatalf("preflight exposed a path: %q", output)
	}
}

func TestLaunchersRejectRelativeBuildPathsAcrossWorkingDirectoryChange(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("launchers are bash scripts")
	}
	cursor, err := filepath.Abs(filepath.Join("..", "..", "scripts", "cursor-mcp-command.sh"))
	if err != nil {
		t.Fatal(err)
	}
	generator, err := filepath.Abs(filepath.Join("..", "..", "scripts", "generate-cerberus-launchers.sh"))
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name   string
		script string
		env    []string
	}{
		{
			name:   "cursor binary and pin",
			script: cursor,
			env: []string{
				"MERISTEM_BIN=relative-meristem-bin",
				"MERISTEM_V1_PIN_FILE=relative-meristem-bin.v1-pin",
			},
		},
		{
			name:   "cerberus generator binary",
			script: generator,
			env:    []string{"MERISTEM_BIN=relative-meristem-bin"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			command := exec.Command("bash", tc.script)
			command.Dir = t.TempDir()
			command.Env = append(os.Environ(), tc.env...)
			output, err := command.CombinedOutput()
			if err == nil || !strings.Contains(string(output), "absolute") {
				t.Fatalf("relative launcher path was not refused: %v: %s", err, output)
			}
		})
	}
}
