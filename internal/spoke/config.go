// Package spoke is the outbound half of a pull-only fleet node
// (docs/network-layer-spec.md §2 "Commands to nodes without inbound
// reachability", §6 stage 1). A spoke has no inbound surface: it reaches the
// hub only by outbound HTTPS. Each tick it
//
//   - drains its durable command queue on the hub
//     (GET /v1/crossnode/commands?target=<node_id>), executes each command
//     against its own local api under its spoke-local agent token with the
//     command's original idempotency key so replays collapse, and acks the
//     structural outcome back (POST /v1/crossnode/commands/{event_id}/ack); and
//   - advances a persisted hub-feed cursor, logging new event counts (no local
//     re-projection of hub events — the remote_refs cache is out of scope).
//
// Nothing here listens. The hub being unreachable is not an error: a tick that
// cannot reach the hub logs a warning and ends cleanly, and the next tick
// retries, so the spoke keeps full local function during a partition.
package spoke

import (
	"fmt"
	"os"
	"strings"

	"github.com/jbmopper/meristem/internal/domain"
)

// Environment variable names. Token values (HubToken, LocalToken) are read
// straight from the environment and are never a file path in code and never
// logged.
const (
	// EnvHubURL is the base URL of the hub this spoke polls (outbound HTTPS).
	EnvHubURL = "MERISTEM_HUB_URL"
	// EnvNodeID is this spoke's stable, DNS-safe node id — the target its
	// command queue is keyed by on the hub.
	EnvNodeID = "MERISTEM_NODE_ID"
	// EnvHubToken is the hub-minted bearer for this spoke's agent (§2
	// "Cross-node identity": one token per agent per node). Read here, sent as
	// the Authorization bearer on hub calls, never logged.
	EnvHubToken = "MERISTEM_HUB_TOKEN"
	// EnvLocalURL is the base URL of this node's own local api. Defaults to
	// DefaultLocalURL.
	EnvLocalURL = "MERISTEM_LOCAL_URL"
	// EnvLocalToken is this node's own agent bearer, used to execute drained
	// commands against the local api. Read here, never logged.
	EnvLocalToken = "MERISTEM_TOKEN"
)

// DefaultLocalURL is the local api base URL when EnvLocalURL is unset.
const DefaultLocalURL = "http://localhost:8080"

// DefaultDrainLimit bounds how many queued commands a single tick drains.
const DefaultDrainLimit = 100

// Config is the resolved spoke configuration. Tokens are held in memory only
// for the process lifetime and are never logged.
type Config struct {
	HubBaseURL string
	NodeID     string
	HubToken   string
	LocalURL   string
	LocalToken string
	DrainLimit int
}

// LoadConfig resolves the spoke configuration from the environment, failing
// closed on any missing required value. Error messages never echo token
// material.
func LoadConfig() (Config, error) {
	cfg := Config{
		HubBaseURL: strings.TrimSpace(os.Getenv(EnvHubURL)),
		NodeID:     strings.TrimSpace(os.Getenv(EnvNodeID)),
		HubToken:   os.Getenv(EnvHubToken),
		LocalURL:   strings.TrimSpace(os.Getenv(EnvLocalURL)),
		LocalToken: os.Getenv(EnvLocalToken),
		DrainLimit: DefaultDrainLimit,
	}
	if cfg.LocalURL == "" {
		cfg.LocalURL = DefaultLocalURL
	}
	if cfg.HubBaseURL == "" {
		return Config{}, fmt.Errorf("spoke: %s is required (the hub base URL to poll)", EnvHubURL)
	}
	if err := domain.ValidateNodeOrigin(cfg.HubBaseURL); err != nil {
		return Config{}, fmt.Errorf("spoke: %s must be a credential-safe origin: %w", EnvHubURL, err)
	}
	if err := domain.ValidateNodeOrigin(cfg.LocalURL); err != nil {
		return Config{}, fmt.Errorf("spoke: %s must be a credential-safe origin: %w", EnvLocalURL, err)
	}
	if cfg.NodeID == "" {
		return Config{}, fmt.Errorf("spoke: %s is required (this node's DNS-safe node id)", EnvNodeID)
	}
	if !domain.ValidNodeID(cfg.NodeID) {
		return Config{}, fmt.Errorf("spoke: %s %q is not a DNS-safe node id", EnvNodeID, cfg.NodeID)
	}
	if strings.TrimSpace(cfg.HubToken) == "" {
		return Config{}, fmt.Errorf("spoke: %s is required (the hub-minted bearer for this spoke agent)", EnvHubToken)
	}
	if strings.TrimSpace(cfg.LocalToken) == "" {
		return Config{}, fmt.Errorf("spoke: %s is required (this node's local agent bearer)", EnvLocalToken)
	}
	return cfg, nil
}
