package nodes

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/peerhttp"
)

const maxRegistrySnapshotBody = 2 << 20

var (
	ErrRegistrySyncConfig   = errors.New("nodes: invalid registry sync configuration")
	ErrRegistrySyncResponse = errors.New("nodes: invalid registry sync response")
)

// RegistrySyncConfig is the bootstrap input for outbound registry
// reconciliation. RegistryHomeToken is a bearer minted by the registry home;
// LocalActor is the authenticated, dedicated token used to append the local
// observed event. Neither value is persisted in the snapshot or logged.
type RegistrySyncConfig struct {
	RegistryHomeOrigin string
	ExpectedSource     string
	RegistryHomeToken  string
	LocalActor         domain.Token
	RequestTimeout     time.Duration
}

// RegistrySyncService fetches the authoritative snapshot over outbound REST
// and folds it into the consumer's own event log. It performs no remote write.
type RegistrySyncService struct {
	cfg       RegistrySyncConfig
	snapshots *SnapshotService
	client    *http.Client
}

// NewRegistrySyncService validates bootstrap identity and constructs a
// request-pinned client. PeerOptions is normally zero; its seams support
// deterministic DNS/TLS tests without weakening production defaults.
func NewRegistrySyncService(snapshots *SnapshotService, cfg RegistrySyncConfig, peerOptions peerhttp.Options) (*RegistrySyncService, error) {
	if snapshots == nil || !domain.ValidNodeID(cfg.ExpectedSource) || strings.TrimSpace(cfg.RegistryHomeToken) == "" {
		return nil, ErrRegistrySyncConfig
	}
	origin, err := domain.CanonicalNodeOrigin(cfg.RegistryHomeOrigin)
	if err != nil {
		return nil, fmt.Errorf("%w: registry home origin", ErrRegistrySyncConfig)
	}
	if cfg.LocalActor.IsRoot || cfg.LocalActor.RevokedAt != nil || (cfg.LocalActor.Source != domain.SourceSystem && cfg.LocalActor.Source != domain.SourceAgent) || !CanObserveSnapshot(cfg.LocalActor, cfg.ExpectedSource) {
		return nil, fmt.Errorf("%w: local actor must be a dedicated non-root system or agent token with %s", ErrRegistrySyncConfig, SnapshotObserveScope(cfg.ExpectedSource))
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = 5 * time.Second
	}
	cfg.RegistryHomeOrigin = origin
	peerOptions.Timeout = cfg.RequestTimeout
	return &RegistrySyncService{cfg: cfg, snapshots: snapshots, client: peerhttp.NewClient(peerOptions)}, nil
}

// RegistrySyncResult describes one reconciliation tick.
type RegistrySyncResult struct {
	SourceRevision int64
	Observed       bool
}

// Tick performs one bounded GET and local Observe. A network or validation
// failure occurs before any local event/projection mutation, preserving the
// last accepted snapshot during registry-home outages.
func (s *RegistrySyncService) Tick(ctx context.Context) (RegistrySyncResult, error) {
	requestCtx, cancel := context.WithTimeout(ctx, s.cfg.RequestTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(requestCtx, http.MethodGet, s.cfg.RegistryHomeOrigin+"/v1/nodes/registry-snapshot", nil)
	if err != nil {
		return RegistrySyncResult{}, fmt.Errorf("nodes: build registry snapshot request: %w", err)
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Authorization", "Bearer "+s.cfg.RegistryHomeToken)

	response, err := s.client.Do(request)
	if err != nil {
		return RegistrySyncResult{}, fmt.Errorf("nodes: fetch registry snapshot: %w", err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		return RegistrySyncResult{}, fmt.Errorf("%w: status %d", ErrRegistrySyncResponse, response.StatusCode)
	}

	limited := io.LimitReader(response.Body, maxRegistrySnapshotBody+1)
	body, err := io.ReadAll(limited)
	if err != nil {
		return RegistrySyncResult{}, fmt.Errorf("%w: read body", ErrRegistrySyncResponse)
	}
	if len(body) > maxRegistrySnapshotBody {
		return RegistrySyncResult{}, fmt.Errorf("%w: body exceeds %d bytes", ErrRegistrySyncResponse, maxRegistrySnapshotBody)
	}
	var incoming RegistrySnapshot
	decoder := json.NewDecoder(strings.NewReader(string(body)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&incoming); err != nil {
		return RegistrySyncResult{}, fmt.Errorf("%w: decode snapshot", ErrRegistrySyncResponse)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return RegistrySyncResult{}, err
	}
	validated, err := NormalizeSnapshot(incoming, s.cfg.ExpectedSource)
	if err != nil {
		return RegistrySyncResult{}, err
	}
	accepted, fresh, err := s.snapshots.Observe(ctx, s.cfg.LocalActor, s.cfg.ExpectedSource, validated)
	if err != nil {
		return RegistrySyncResult{}, err
	}
	return RegistrySyncResult{SourceRevision: accepted.SourceRevision, Observed: fresh}, nil
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: trailing JSON", ErrRegistrySyncResponse)
	}
	return nil
}
