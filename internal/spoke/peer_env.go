package spoke

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/domain"
)

// EnvPeerTokenPrefix names the per-peer bearer variables. One variable per
// queue host, e.g. MERISTEM_PEER_TOKEN_HUB for peer "hub" and
// MERISTEM_PEER_TOKEN_HOME_SERVER for peer "home-server".
//
// Separate variables, rather than one map-shaped value, so a supervisor or
// secret manager can inject, rotate, and scope each peer's credential
// independently — and so a mistake in one peer's material cannot corrupt the
// parse of another's.
const EnvPeerTokenPrefix = "MERISTEM_PEER_TOKEN_"

// EnvPeerTokenName returns the environment variable holding peerNodeID's
// bearer. Node ids are lowercase DNS labels and environment variables are
// conventionally uppercase with underscores, so the hyphen is mapped rather
// than rejected.
//
// The mapping is checked for collisions by the caller-facing resolver: node ids
// cannot contain underscores, so two distinct ids can never map to the same
// variable name.
func EnvPeerTokenName(peerNodeID string) (string, error) {
	if !domain.ValidNodeID(peerNodeID) {
		return "", fmt.Errorf("spoke: %q is not a DNS-safe node id", peerNodeID)
	}
	return EnvPeerTokenPrefix + strings.ToUpper(strings.ReplaceAll(peerNodeID, "-", "_")), nil
}

// EnvBearerResolver resolves each peer's credential from its own environment
// variable at call time, so a supervisor that re-executes the process picks up
// rotated material without a code change.
//
// The returned error never contains the variable's value, and never says
// whether the variable was absent or merely empty — an error that distinguishes
// those is a probe for which peers this node holds credentials for.
func EnvBearerResolver() PeerBearerResolver {
	return func(_ context.Context, peerNodeID string) (string, error) {
		name, err := EnvPeerTokenName(peerNodeID)
		if err != nil {
			return "", err
		}
		value := strings.TrimSpace(os.Getenv(name))
		if value == "" {
			return "", fmt.Errorf("spoke: no credential configured for peer %q", peerNodeID)
		}
		return value, nil
	}
}

// projectionPeerSource resolves drain peers from the local nodes projection on
// every tick, so an operator's registry change takes effect at the next tick
// without restarting the spoke.
type projectionPeerSource struct {
	pool        *pgxpool.Pool
	localNodeID string
	grace       time.Duration
	now         func() time.Time
}

// NewProjectionPeerSource builds the production PeerSource. A zero grace uses
// DefaultDrainGrace; see ResolveDrainPeers for why the drain set is wider than
// the current allowlist.
func NewProjectionPeerSource(pool *pgxpool.Pool, localNodeID string, grace time.Duration) (PeerSource, error) {
	if pool == nil {
		return nil, fmt.Errorf("spoke: peer source requires a database pool")
	}
	if !domain.ValidNodeID(localNodeID) {
		return nil, fmt.Errorf("spoke: %q is not a DNS-safe node id", localNodeID)
	}
	if grace < 0 {
		return nil, fmt.Errorf("spoke: drain grace must not be negative, got %s", grace)
	}
	if grace == 0 {
		grace = DefaultDrainGrace
	}
	return &projectionPeerSource{pool: pool, localNodeID: localNodeID, grace: grace, now: time.Now}, nil
}

func (s *projectionPeerSource) DrainPeers(ctx context.Context) ([]DrainPeer, error) {
	return ResolveDrainPeers(ctx, s.pool, s.localNodeID, s.now(), s.grace)
}
