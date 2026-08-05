package nodes

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
)

const SnapshotPayloadVersion = 1

var (
	ErrSnapshotDenied      = errors.New("nodes: registry snapshot access denied")
	ErrInvalidSnapshot     = errors.New("nodes: invalid registry snapshot")
	ErrWrongSnapshotSource = errors.New("nodes: wrong registry snapshot source")
	ErrStaleSnapshot       = errors.New("nodes: stale registry snapshot")
	ErrSnapshotConflict    = errors.New("nodes: registry snapshot revision conflict")
)

// SnapshotEntry is one complete routing-intent row from the registry home.
// RegistryRevision is an events.seq cursor meaningful only at SourceNodeID.
//
// QueueVia serializes as relay_via, and the struct deliberately has no
// queue_via field at all. This is the exact opposite of the node event wire,
// which emits both spellings — and the asymmetry is load-bearing. Snapshot
// consumers decode with DisallowUnknownFields (registry_sync.go), so an
// additive queue_via key does not get ignored by an old peer: it rejects the
// entire snapshot and cross-node registry sync hard-fails. Additive fields are
// only safe where the decoder tolerates unknown ones. queue_via reaches this
// wire when a negotiated snapshot v2 exists, not before — and bumping
// SnapshotPayloadVersion alone is not that negotiation.
//
// UnmarshalJSON still ACCEPTS either spelling on input, so a peer already
// emitting queue_via is understood; only output is constrained.
type SnapshotEntry struct {
	NodeID           string            `json:"node_id"`
	BaseURL          *string           `json:"base_url,omitempty"`
	DirectURL        *string           `json:"direct_url,omitempty"`
	QueueVia         []string          `json:"relay_via"`
	Status           domain.NodeStatus `json:"status"`
	RegistryRevision int64             `json:"registry_revision"`
}

func (e *SnapshotEntry) UnmarshalJSON(data []byte) error {
	type alias struct {
		NodeID           string            `json:"node_id"`
		BaseURL          *string           `json:"base_url,omitempty"`
		DirectURL        *string           `json:"direct_url,omitempty"`
		QueueVia         *[]string         `json:"queue_via"`
		LegacyRelayVia   *[]string         `json:"relay_via"`
		Status           domain.NodeStatus `json:"status"`
		RegistryRevision int64             `json:"registry_revision"`
	}
	// A custom UnmarshalJSON bypasses the caller's DisallowUnknownFields, so
	// re-assert it here or entries would silently become the one lax spot in an
	// otherwise strict decode path.
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var raw alias
	if err := dec.Decode(&raw); err != nil {
		return err
	}
	resolved, err := resolveQueueVia(raw.QueueVia, raw.LegacyRelayVia)
	if err != nil {
		return fmt.Errorf("registry snapshot entry %q: %w", raw.NodeID, err)
	}
	*e = SnapshotEntry{
		NodeID:           raw.NodeID,
		BaseURL:          raw.BaseURL,
		DirectURL:        raw.DirectURL,
		QueueVia:         resolved,
		Status:           raw.Status,
		RegistryRevision: raw.RegistryRevision,
	}
	return nil
}

// RegistrySnapshot is the authenticated, complete registry distribution unit.
type RegistrySnapshot struct {
	PayloadVersion int             `json:"payload_version"`
	SourceNodeID   string          `json:"source_node_id"`
	SourceRevision int64           `json:"source_revision"`
	Nodes          []SnapshotEntry `json:"nodes"`
}

type SnapshotService struct {
	pool   *pgxpool.Pool
	writer *events.Writer
}

func NewSnapshotService(pool *pgxpool.Pool, writer *events.Writer) *SnapshotService {
	return &SnapshotService{pool: pool, writer: writer}
}

func SnapshotReadScope(sourceNodeID string) string {
	return "registry.snapshot.read:" + sourceNodeID
}

func SnapshotObserveScope(sourceNodeID string) string {
	return "registry.snapshot.observe:" + sourceNodeID
}

func hasExactScope(actor domain.Token, want string) bool {
	if actor.IsRoot || actor.RevokedAt != nil {
		return false
	}
	for _, scope := range actor.Scopes {
		if strings.TrimSpace(scope) == want {
			return true
		}
	}
	return false
}

func CanReadSnapshot(actor domain.Token, sourceNodeID string) bool {
	return hasExactScope(actor, SnapshotReadScope(sourceNodeID))
}

func CanObserveSnapshot(actor domain.Token, sourceNodeID string) bool {
	return hasExactScope(actor, SnapshotObserveScope(sourceNodeID))
}

// Build returns a consistent authoritative snapshot. A fleet peer needs an
// explicit source-bound read scope; root and legacy broad tokens never cross
// this peer-delivery boundary.
func (s *SnapshotService) Build(ctx context.Context, actor domain.Token, sourceNodeID string) (RegistrySnapshot, error) {
	if !CanReadSnapshot(actor, sourceNodeID) {
		return RegistrySnapshot{}, ErrSnapshotDenied
	}
	if !domain.ValidNodeID(sourceNodeID) {
		return RegistrySnapshot{}, fmt.Errorf("%w: source_node_id", ErrInvalidSnapshot)
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return RegistrySnapshot{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var revision int64
	if err := tx.QueryRow(ctx, `SELECT COALESCE(MAX(seq), 0) FROM events`).Scan(&revision); err != nil {
		return RegistrySnapshot{}, err
	}
	// relay_via, not queue_via, for the expand window — see nodes.List.
	rows, err := tx.Query(ctx, `
		SELECT node_id, base_url, direct_url, relay_via, status, registry_revision
		FROM nodes ORDER BY node_id
	`)
	if err != nil {
		return RegistrySnapshot{}, err
	}
	var entries []SnapshotEntry
	for rows.Next() {
		var entry SnapshotEntry
		var relayJSON []byte
		if err := rows.Scan(&entry.NodeID, &entry.BaseURL, &entry.DirectURL, &relayJSON, &entry.Status, &entry.RegistryRevision); err != nil {
			rows.Close()
			return RegistrySnapshot{}, err
		}
		if err := json.Unmarshal(relayJSON, &entry.QueueVia); err != nil {
			rows.Close()
			return RegistrySnapshot{}, err
		}
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return RegistrySnapshot{}, err
	}
	rows.Close()

	snapshot, err := NormalizeSnapshot(RegistrySnapshot{
		PayloadVersion: SnapshotPayloadVersion,
		SourceNodeID:   sourceNodeID,
		SourceRevision: revision,
		Nodes:          entries,
	}, sourceNodeID)
	if err != nil {
		return RegistrySnapshot{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return RegistrySnapshot{}, err
	}
	return snapshot, nil
}

// Observe validates before append, serializes revision checks, and appends one
// event whose projector replaces both routing rows and accepted cursor.
func (s *SnapshotService) Observe(ctx context.Context, actor domain.Token, expectedSource string, incoming RegistrySnapshot) (RegistrySnapshot, bool, error) {
	if !CanObserveSnapshot(actor, expectedSource) {
		return RegistrySnapshot{}, false, ErrSnapshotDenied
	}
	snapshot, err := NormalizeSnapshot(incoming, expectedSource)
	if err != nil {
		return RegistrySnapshot{}, false, err
	}
	digest, err := snapshotDigest(snapshot)
	if err != nil {
		return RegistrySnapshot{}, false, err
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return RegistrySnapshot{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	// The state row does not exist before the first event, so a table lock is
	// required to serialize two concurrent first observations.
	if _, err := tx.Exec(ctx, `LOCK TABLE registry_snapshot_state IN EXCLUSIVE MODE`); err != nil {
		return RegistrySnapshot{}, false, err
	}
	var currentSource string
	var currentRevision int64
	var currentDigest []byte
	err = tx.QueryRow(ctx, `SELECT source_node_id, source_revision, snapshot_digest FROM registry_snapshot_state WHERE singleton`).Scan(&currentSource, &currentRevision, &currentDigest)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return RegistrySnapshot{}, false, err
	}
	if err == nil {
		if currentSource != expectedSource {
			return RegistrySnapshot{}, false, ErrWrongSnapshotSource
		}
		if snapshot.SourceRevision == currentRevision {
			if bytes.Equal(digest, currentDigest) {
				return snapshot, false, nil
			}
			return RegistrySnapshot{}, false, ErrSnapshotConflict
		}
		if snapshot.SourceRevision < currentRevision {
			return RegistrySnapshot{}, false, ErrStaleSnapshot
		}
	}

	_, fresh, err := s.writer.Append(ctx, tx, events.Spec{
		SubjectKind:  domain.SubjectRegistrySnapshot,
		SubjectID:    SnapshotSubjectID(expectedSource),
		Kind:         domain.EventRegistrySnapshotObserved,
		Source:       actor.Source,
		ActorTokenID: &actor.ID,
		Payload:      snapshot,
	})
	if err != nil {
		return RegistrySnapshot{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return RegistrySnapshot{}, false, err
	}
	return snapshot, fresh, nil
}

// NormalizeSnapshot performs the complete deterministic reducer before an
// observed event can exist. It canonicalizes origins and entry order, and
// refuses incomplete/ambiguous topology.
func NormalizeSnapshot(in RegistrySnapshot, expectedSource string) (RegistrySnapshot, error) {
	if !domain.ValidNodeID(expectedSource) || in.SourceNodeID != expectedSource {
		return RegistrySnapshot{}, ErrWrongSnapshotSource
	}
	if in.PayloadVersion != SnapshotPayloadVersion || in.SourceRevision <= 0 || len(in.Nodes) == 0 {
		return RegistrySnapshot{}, ErrInvalidSnapshot
	}
	out := RegistrySnapshot{
		PayloadVersion: SnapshotPayloadVersion,
		SourceNodeID:   in.SourceNodeID,
		SourceRevision: in.SourceRevision,
		Nodes:          make([]SnapshotEntry, len(in.Nodes)),
	}
	ids := make(map[string]bool, len(in.Nodes))
	for i, entry := range in.Nodes {
		if !domain.ValidNodeID(entry.NodeID) || ids[entry.NodeID] || entry.RegistryRevision <= 0 || entry.RegistryRevision > in.SourceRevision {
			return RegistrySnapshot{}, ErrInvalidSnapshot
		}
		if entry.Status != domain.NodeStatusActive && entry.Status != domain.NodeStatusDisabled {
			return RegistrySnapshot{}, ErrInvalidSnapshot
		}
		ids[entry.NodeID] = true
		base, err := canonicalSnapshotOrigin(entry.BaseURL)
		if err != nil {
			return RegistrySnapshot{}, err
		}
		direct, err := canonicalSnapshotOrigin(entry.DirectURL)
		if err != nil {
			return RegistrySnapshot{}, err
		}
		qv := append([]string(nil), entry.QueueVia...)
		if qv == nil {
			qv = []string{}
		}
		seenQV := make(map[string]bool, len(qv))
		for _, target := range qv {
			if !domain.ValidNodeID(target) || target == entry.NodeID || seenQV[target] {
				return RegistrySnapshot{}, ErrInvalidSnapshot
			}
			seenQV[target] = true
		}
		out.Nodes[i] = SnapshotEntry{NodeID: entry.NodeID, BaseURL: base, DirectURL: direct, QueueVia: qv, Status: entry.Status, RegistryRevision: entry.RegistryRevision}
	}
	if !ids[in.SourceNodeID] {
		return RegistrySnapshot{}, ErrInvalidSnapshot
	}
	for _, entry := range out.Nodes {
		for _, target := range entry.QueueVia {
			if !ids[target] {
				return RegistrySnapshot{}, ErrInvalidSnapshot
			}
		}
	}
	sort.Slice(out.Nodes, func(i, j int) bool { return out.Nodes[i].NodeID < out.Nodes[j].NodeID })
	return out, nil
}

func canonicalSnapshotOrigin(raw *string) (*string, error) {
	if raw == nil {
		return nil, nil
	}
	value, err := domain.CanonicalNodeOrigin(*raw)
	if err != nil {
		return nil, fmt.Errorf("%w: origin", ErrInvalidSnapshot)
	}
	return &value, nil
}

var snapshotNamespace = uuid.MustParse("7d07c5d0-319a-5ef1-83a0-d8db40c1bf62")

func SnapshotSubjectID(sourceNodeID string) uuid.UUID {
	return uuid.NewSHA1(snapshotNamespace, []byte("registry-snapshot|"+sourceNodeID))
}

func snapshotDigest(snapshot RegistrySnapshot) ([]byte, error) {
	canonical, err := events.CanonicalJSON(snapshot)
	if err != nil {
		return nil, fmt.Errorf("%w: canonical payload", ErrInvalidSnapshot)
	}
	sum := sha256.Sum256(canonical)
	return sum[:], nil
}
