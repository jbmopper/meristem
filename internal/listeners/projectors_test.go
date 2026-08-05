package listeners

// LCP2-B4 regression: every listener projector fails replay closed on a
// payload_version it was not built for, BEFORE any projection write. The
// unknown-version cases run Apply with a nil transaction on purpose — if the
// version gate ever moved after the first tx use, these would panic instead
// of returning the typed error.

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/projections"
)

func listenerEvent(kind string, payload map[string]any) domain.Event {
	return domain.Event{
		ID:          uuid.New(),
		SubjectKind: domain.SubjectListener,
		SubjectID:   uuid.New(),
		Kind:        kind,
		Payload:     payload,
		Seq:         1,
	}
}

func TestEventPayloadVersionDispatch(t *testing.T) {
	cases := []struct {
		name    string
		payload map[string]any
		want    int
		wantErr bool
	}{
		{"absent version is v1", map[string]any{"name": "x"}, 1, false},
		{"null version is v1", map[string]any{"payload_version": nil}, 1, false},
		{"explicit v1", map[string]any{"payload_version": 1}, 1, false},
		{"unknown v99 decodes for the gate to refuse", map[string]any{"payload_version": 99}, 99, false},
		{"fractional version is malformed", map[string]any{"payload_version": 1.5}, 0, true},
		{"string version is malformed", map[string]any{"payload_version": "one"}, 0, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := eventPayloadVersion(listenerEvent(domain.EventListenerRegistered, c.payload))
			if c.wantErr {
				if err == nil {
					t.Fatalf("want error, got version %d", got)
				}
				return
			}
			if err != nil || got != c.want {
				t.Fatalf("version = %d (%v), want %d", got, err, c.want)
			}
		})
	}
}

func TestProjectorsRefuseUnknownPayloadVersions(t *testing.T) {
	registry := projections.NewRegistry()
	RegisterProjectors(registry)
	kinds := []string{
		domain.EventListenerRegistered,
		domain.EventListenerCredentialBound,
		domain.EventListenerPolicySet,
		domain.EventListenerRetired,
	}
	for _, kind := range kinds {
		for _, payload := range []map[string]any{
			{"payload_version": 99},
			{"payload_version": "two"},
		} {
			err := registry.Apply(context.Background(), nil, listenerEvent(kind, payload))
			if err == nil {
				t.Errorf("%s: payload %v projected instead of failing closed", kind, payload)
				continue
			}
			if !strings.Contains(err.Error(), "payload_version") {
				t.Errorf("%s: error does not name the version gate: %v", kind, err)
			}
		}
	}
}
