package api

import (
	"net/http"
	"strings"
	"testing"
)

// REST shares the append seam with MCP; one pair of cases pins the transport:
// an object payload lands, a double-encoded string is rejected with the
// precise message and a 400.
func TestRESTAppendEventPayloadShapeBoundaryIntegration(t *testing.T) {
	fixture := newAssignedFeedFixture(t)
	path := "/v1/work-items/" + fixture.unassigned.ID.String() + "/events"

	rec := doREST(t, fixture.server.Handler(), http.MethodPost, path, fixture.root.Secret, "shape-rest-object",
		[]byte(`{"kind":"agent.rest_shape_object","payload":{"marker":"rest-object"}}`))
	assertRESTStatus(t, rec, http.StatusCreated)

	rec = doREST(t, fixture.server.Handler(), http.MethodPost, path, fixture.root.Secret, "shape-rest-double",
		[]byte(`{"kind":"agent.rest_shape_double","payload":"{\"marker\":\"double\"}"}`))
	assertRESTStatus(t, rec, http.StatusBadRequest)
	if !strings.Contains(rec.Body.String(), "double-encoded") {
		t.Fatalf("REST double-encoded rejection lacks the precise message: %s", rec.Body.String())
	}
	var innerType string
	if err := fixture.pool.QueryRow(fixture.ctx, `
		SELECT jsonb_typeof(payload->'inner') FROM events
		WHERE subject_id=$1 AND payload->>'inner_kind'='agent.rest_shape_object'`, fixture.unassigned.ID).Scan(&innerType); err != nil {
		t.Fatalf("read inner type: %v", err)
	}
	if innerType != "object" {
		t.Fatalf("REST-written inner type = %s, want object", innerType)
	}
}
