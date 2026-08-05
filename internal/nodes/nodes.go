// Package nodes owns the fleet node registry projection.
//
// Truth is the node.registered / node.route_updated event stream on this
// node's own log; the `nodes` table is a current-state projection folded by
// the writers in this package (see projectors.go). It records how each fleet
// node is reached — its registered ingress base_url, an optional direct peer
// route, a queue-host allowlist, and a reachability status — so route selection and
// qualified-ref resolution can read one indexed row instead of the log.
//
// This is the stage 0 slice of docs/network-layer-spec.md: adopt the node_id
// and the projection cheaply now so every future cross-node reference is
// stable. There is no service/API surface yet; the append path lands with the
// stage 1 cross-node work.
package nodes

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/google/uuid"
)

// subjectNamespace seeds the deterministic node subject ids. A fixed random
// UUID keeps NodeSubjectID pure and stable across processes and rebuilds.
var subjectNamespace = uuid.MustParse("b8f2e0a4-3c1d-5e6f-8a9b-0c1d2e3f4a5b")

// routePayloadVersion 2 introduced credential-safe canonical node origins.
// Version 1 remains replayable because historical events legitimately contain
// private/plaintext development origins that were accepted by the old writer.
const routePayloadVersion = 2

// NodeSubjectID derives the deterministic event subject id for a node from its
// node_id, mirroring internal/registry's name→subject derivation. Every event
// about one node shares this subject while the projection keys on node_id.
func NodeSubjectID(nodeID string) uuid.UUID {
	return uuid.NewSHA1(subjectNamespace, []byte("node|"+nodeID))
}

// registeredPayload is the field-minimal structural payload of a
// node.registered event (docs/cerberus-reducer-event-contracts.md: a field is
// structural iff deterministic code must read it without parsing prose).
//
// base_url, direct_url, and queue_via are optional: an inbound-less node may
// register with none of them and be reached only by outbound polling. Unknown
// fields are tolerated (payload versioning: additive fields do not bump).
type registeredPayload struct {
	PayloadVersion int     `json:"payload_version,omitempty"`
	NodeID         string  `json:"node_id"`
	BaseURL        *string `json:"base_url,omitempty"`
	DirectURL      *string `json:"direct_url,omitempty"`
	// QueueVia and LegacyRelayVia carry the same value on the wire during the
	// expand window (see resolveQueueVia). Writers populate both; UnmarshalJSON
	// collapses whatever a producer sent back into QueueVia.
	QueueVia       []string `json:"queue_via,omitempty"`
	LegacyRelayVia []string `json:"relay_via,omitempty"`
	Status         string   `json:"status"`
}

func (p *registeredPayload) UnmarshalJSON(data []byte) error {
	type alias registeredPayload
	var raw struct {
		alias
		QueueVia       *[]string `json:"queue_via"`
		LegacyRelayVia *[]string `json:"relay_via"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	resolved, err := resolveQueueVia(raw.QueueVia, raw.LegacyRelayVia)
	if err != nil {
		return fmt.Errorf("node.registered: %w", err)
	}
	*p = registeredPayload(raw.alias)
	p.QueueVia = resolved
	p.LegacyRelayVia = resolved
	return nil
}

// routeUpdatedPayload is the field-minimal structural payload of a
// node.route_updated event. It carries the full replacement route state the
// projector sets — direct_url, queue_via, status — keyed by node_id. base_url
// is deliberately absent: registration owns the ingress URL; a route update
// never rewrites it.
type routeUpdatedPayload struct {
	PayloadVersion int     `json:"payload_version,omitempty"`
	NodeID         string  `json:"node_id"`
	DirectURL      *string `json:"direct_url,omitempty"`
	// See registeredPayload: both keys carry the same value during expand.
	QueueVia       []string `json:"queue_via,omitempty"`
	LegacyRelayVia []string `json:"relay_via,omitempty"`
	Status         string   `json:"status"`
}

func (p *routeUpdatedPayload) UnmarshalJSON(data []byte) error {
	type alias routeUpdatedPayload
	var raw struct {
		alias
		QueueVia       *[]string `json:"queue_via"`
		LegacyRelayVia *[]string `json:"relay_via"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	resolved, err := resolveQueueVia(raw.QueueVia, raw.LegacyRelayVia)
	if err != nil {
		return fmt.Errorf("node.route_updated: %w", err)
	}
	*p = routeUpdatedPayload(raw.alias)
	p.QueueVia = resolved
	p.LegacyRelayVia = resolved
	return nil
}

// ErrAmbiguousQueueVia rejects a payload that carries both the canonical
// queue_via key and the legacy relay_via key with different values. During the
// expand window a compatibility producer emits both with equal values, so
// disagreement means two writers raced or a payload was hand-edited; there is
// no safe way to pick a winner, and guessing would silently reroute traffic.
var ErrAmbiguousQueueVia = errors.New("nodes: queue_via and relay_via disagree")

// resolveQueueVia collapses the two wire spellings of the queue-host allowlist
// into the canonical value.
//
// Selection is by field PRESENCE, not by slice length: an explicit
// "queue_via": [] means "this node has no queue hosts" and must not fall back
// to a stale relay_via. A nil pointer means the key was absent.
func resolveQueueVia(queueVia, legacyRelayVia *[]string) ([]string, error) {
	switch {
	case queueVia != nil && legacyRelayVia != nil:
		if !slices.Equal(*queueVia, *legacyRelayVia) {
			return nil, fmt.Errorf("%w: queue_via=%v relay_via=%v", ErrAmbiguousQueueVia, *queueVia, *legacyRelayVia)
		}
		return *queueVia, nil
	case queueVia != nil:
		return *queueVia, nil
	case legacyRelayVia != nil:
		return *legacyRelayVia, nil
	default:
		return nil, nil
	}
}

// payloadVersion reads the payload_version field, treating absence as 1 per
// docs/payload-versioning.md. A malformed value falls through to 1 so the
// version switch (not this helper) is the single place that fails closed.
func payloadVersion(raw any) int {
	b, err := json.Marshal(raw)
	if err != nil {
		return 1
	}
	var probe struct {
		PayloadVersion int `json:"payload_version"`
	}
	if err := json.Unmarshal(b, &probe); err != nil || probe.PayloadVersion == 0 {
		return 1
	}
	return probe.PayloadVersion
}

func decode(raw any, dst any) error {
	b, err := json.Marshal(raw)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, dst)
}
