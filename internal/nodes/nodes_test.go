package nodes

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/domain"
)

func registeredEvent(payload any) domain.Event {
	return domain.Event{
		ID:          uuid.New(),
		SubjectKind: domain.SubjectNode,
		SubjectID:   NodeSubjectID("m4"),
		Kind:        domain.EventNodeRegistered,
		Source:      domain.SourceSystem,
		OccurredAt:  time.Unix(0, 0).UTC(),
		Payload:     payload,
	}
}

func routeUpdatedEvent(payload any) domain.Event {
	return domain.Event{
		ID:          uuid.New(),
		SubjectKind: domain.SubjectNode,
		SubjectID:   NodeSubjectID("m4"),
		Kind:        domain.EventNodeRouteUpdated,
		Source:      domain.SourceSystem,
		OccurredAt:  time.Unix(0, 0).UTC(),
		Payload:     payload,
	}
}

// The projectors validate the payload before touching the transaction, so a
// nil tx is a fine probe: a validation error must come back without a panic.

func TestRegisteredProjectorRejectsWrongSubjectKind(t *testing.T) {
	ev := registeredEvent(map[string]any{"node_id": "m4", "status": "active"})
	ev.SubjectKind = domain.SubjectWorkItem
	err := registeredProjector{}.Apply(context.Background(), nil, ev)
	if err == nil || !strings.Contains(err.Error(), "subject_kind") {
		t.Fatalf("expected subject_kind error, got %v", err)
	}
}

func TestRegisteredProjectorRejectsBadNodeID(t *testing.T) {
	for _, id := range []string{"", "M4", "-m4", "m4-", "m4:den", "node.one"} {
		ev := registeredEvent(map[string]any{"node_id": id, "status": "active"})
		err := registeredProjector{}.Apply(context.Background(), nil, ev)
		if err == nil || !strings.Contains(err.Error(), "DNS-safe") {
			t.Fatalf("node_id %q: expected DNS-safe error, got %v", id, err)
		}
	}
}

func TestRegisteredProjectorRequiresStatus(t *testing.T) {
	ev := registeredEvent(map[string]any{"node_id": "m4"})
	err := registeredProjector{}.Apply(context.Background(), nil, ev)
	if err == nil || !strings.Contains(err.Error(), "status is required") {
		t.Fatalf("expected status required, got %v", err)
	}
}

func TestRegisteredProjectorRejectsBadQueueHop(t *testing.T) {
	ev := registeredEvent(map[string]any{
		"node_id":   "m4",
		"status":    "active",
		"relay_via": []string{"den", "BAD"},
	})
	err := registeredProjector{}.Apply(context.Background(), nil, ev)
	if err == nil || !strings.Contains(err.Error(), "queue_via[1]") {
		t.Fatalf("expected queue_via hop error, got %v", err)
	}
}

func TestRegisteredProjectorFailsClosedOnUnknownVersion(t *testing.T) {
	ev := registeredEvent(map[string]any{
		"payload_version": 3,
		"node_id":         "m4",
		"status":          "active",
	})
	err := registeredProjector{}.Apply(context.Background(), nil, ev)
	if err == nil || !strings.Contains(err.Error(), "unknown payload_version 3") {
		t.Fatalf("expected fail-closed on unknown version, got %v", err)
	}
}

func TestStrictProjectorsRejectLegacyPrivateHTTPOrigins(t *testing.T) {
	registered := registeredEvent(map[string]any{
		"payload_version": routePayloadVersion,
		"node_id":         "m4",
		"base_url":        "http://10.0.0.63:8080",
		"status":          "active",
	})
	if err := (registeredProjector{}).Apply(context.Background(), nil, registered); err == nil || !strings.Contains(err.Error(), "invalid node origin") {
		t.Fatalf("v2 registration accepted private HTTP origin: %v", err)
	}
	route := routeUpdatedEvent(map[string]any{
		"payload_version": routePayloadVersion,
		"node_id":         "m4",
		"direct_url":      "http://10.0.0.63:8080",
		"status":          "active",
	})
	if err := (routeUpdatedProjector{}).Apply(context.Background(), nil, route); err == nil || !strings.Contains(err.Error(), "invalid node origin") {
		t.Fatalf("v2 route update accepted private HTTP origin: %v", err)
	}
}

func TestRouteUpdatedProjectorRejectsWrongSubjectKind(t *testing.T) {
	ev := routeUpdatedEvent(map[string]any{"node_id": "m4", "status": "active"})
	ev.SubjectKind = domain.SubjectWorkItem
	err := routeUpdatedProjector{}.Apply(context.Background(), nil, ev)
	if err == nil || !strings.Contains(err.Error(), "subject_kind") {
		t.Fatalf("expected subject_kind error, got %v", err)
	}
}

func TestRouteUpdatedProjectorRejectsBadNodeID(t *testing.T) {
	ev := routeUpdatedEvent(map[string]any{"node_id": "Bad", "status": "active"})
	err := routeUpdatedProjector{}.Apply(context.Background(), nil, ev)
	if err == nil || !strings.Contains(err.Error(), "DNS-safe") {
		t.Fatalf("expected DNS-safe error, got %v", err)
	}
}

func TestRouteUpdatedProjectorFailsClosedOnUnknownVersion(t *testing.T) {
	ev := routeUpdatedEvent(map[string]any{
		"payload_version": 9,
		"node_id":         "m4",
		"status":          "active",
	})
	err := routeUpdatedProjector{}.Apply(context.Background(), nil, ev)
	if err == nil || !strings.Contains(err.Error(), "unknown payload_version 9") {
		t.Fatalf("expected fail-closed on unknown version, got %v", err)
	}
}

func TestPayloadVersionAbsentIsOne(t *testing.T) {
	if got := payloadVersion(map[string]any{"node_id": "m4"}); got != 1 {
		t.Fatalf("absent payload_version = %d, want 1", got)
	}
	if got := payloadVersion(map[string]any{"payload_version": 3}); got != 3 {
		t.Fatalf("payload_version = %d, want 3", got)
	}
}

func TestNodeSubjectIDStable(t *testing.T) {
	if NodeSubjectID("m4") != uuid.NewSHA1(subjectNamespace, []byte("node|m4")) {
		t.Fatal("node subject id derivation drifted")
	}
	if NodeSubjectID("m4") == NodeSubjectID("den") {
		t.Fatal("distinct node ids must not collide")
	}
}

func TestNormalizeQueueViaDefaultsEmpty(t *testing.T) {
	b, err := normalizeQueueVia(nil)
	if err != nil {
		t.Fatalf("normalizeQueueVia(nil): %v", err)
	}
	if string(b) != "[]" {
		t.Fatalf("nil queue_via encoded as %q, want []", b)
	}
}
