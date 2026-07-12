package spoke

import (
	"strings"
	"testing"
)

func setRequiredConfig(t *testing.T) {
	t.Helper()
	t.Setenv(EnvHubURL, "https://hub.example")
	t.Setenv(EnvNodeID, "m4")
	t.Setenv(EnvHubToken, "hub-token")
	t.Setenv(EnvLocalURL, "http://127.0.0.1:8080")
	t.Setenv(EnvLocalToken, "local-token")
}

func TestLoadConfigAcceptsCredentialSafeOrigins(t *testing.T) {
	setRequiredConfig(t)
	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.HubBaseURL != "https://hub.example" || cfg.LocalURL != "http://127.0.0.1:8080" {
		t.Fatalf("unexpected origins: hub=%q local=%q", cfg.HubBaseURL, cfg.LocalURL)
	}
}

func TestLoadConfigRejectsUnsafeOrigins(t *testing.T) {
	for _, tc := range []struct {
		name  string
		env   string
		value string
	}{
		{name: "hub path", env: EnvHubURL, value: "https://hub.example/mcp"},
		{name: "hub userinfo", env: EnvHubURL, value: "https://user:pass@hub.example"},
		{name: "local private plaintext", env: EnvLocalURL, value: "http://10.0.0.63:8080"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setRequiredConfig(t)
			t.Setenv(tc.env, tc.value)
			_, err := LoadConfig()
			if err == nil || !strings.Contains(err.Error(), "invalid node origin") {
				t.Fatalf("LoadConfig error = %v", err)
			}
		})
	}
}
