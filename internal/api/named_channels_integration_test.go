package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// Channel identity across transports: a cursor issued under one filter
// identity must be rejected fail-closed under any other, and honored under
// its own, on long-poll and SSE resume alike.
func TestNamedChannelCursorIdentityFailsClosedIntegration(t *testing.T) {
	fixture := newAssignedFeedFixture(t)
	broad := newQuietSelfBroadReader(t, fixture, "named-channel-broad")
	filterParam := "exclude_actor=" + fixture.actorB.Token.ID.String()

	rec := doREST(t, fixture.server.Handler(), http.MethodGet, "/v1/feed?wait=0s&"+filterParam, broad.Secret, "", nil)
	assertRESTStatus(t, rec, http.StatusOK)
	var page struct {
		NextCursor string `json:"next_cursor"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &page); err != nil || page.NextCursor == "" {
		t.Fatalf("no filtered cursor issued: err=%v body=%s", err, rec.Body.String())
	}

	replayed := doREST(t, fixture.server.Handler(), http.MethodGet, "/v1/feed?wait=0s&cursor="+page.NextCursor+"&"+filterParam, broad.Secret, "", nil)
	assertRESTStatus(t, replayed, http.StatusOK)

	unfiltered := doREST(t, fixture.server.Handler(), http.MethodGet, "/v1/feed?wait=0s&cursor="+page.NextCursor, broad.Secret, "", nil)
	assertRESTStatus(t, unfiltered, http.StatusBadRequest)
	assertErrorCode(t, unfiltered, "cursor_filter_mismatch")

	crossFiltered := doREST(t, fixture.server.Handler(), http.MethodGet,
		"/v1/feed?wait=0s&cursor="+page.NextCursor+"&exclude_actor="+fixture.actorA.Token.ID.String(), broad.Secret, "", nil)
	assertRESTStatus(t, crossFiltered, http.StatusBadRequest)
	assertErrorCode(t, crossFiltered, "cursor_filter_mismatch")

	plain := doREST(t, fixture.server.Handler(), http.MethodGet, "/v1/feed?wait=0s", broad.Secret, "", nil)
	assertRESTStatus(t, plain, http.StatusOK)
	var plainPage struct {
		NextCursor string `json:"next_cursor"`
	}
	if err := json.Unmarshal(plain.Body.Bytes(), &plainPage); err != nil || plainPage.NextCursor == "" {
		t.Fatalf("no plain cursor issued: %s", plain.Body.String())
	}
	widened := doREST(t, fixture.server.Handler(), http.MethodGet, "/v1/feed?wait=0s&cursor="+plainPage.NextCursor+"&"+filterParam, broad.Secret, "", nil)
	assertRESTStatus(t, widened, http.StatusBadRequest)
	assertErrorCode(t, widened, "cursor_filter_mismatch")

	// SSE: a filtered cursor replayed without its filter fails before headers.
	req := httptest.NewRequest(http.MethodGet, "/v1/feed/stream?cursor="+page.NextCursor, nil)
	req.Header.Set("Authorization", "Bearer "+broad.Secret)
	streamRec := httptest.NewRecorder()
	fixture.server.Handler().ServeHTTP(streamRec, req)
	assertRESTStatus(t, streamRec, http.StatusBadRequest)
	assertErrorCode(t, streamRec, "cursor_filter_mismatch")
	if strings.HasPrefix(streamRec.Header().Get("Content-Type"), "text/event-stream") {
		t.Fatalf("mismatched cursor committed SSE headers: %v", streamRec.Header())
	}

	// The assigned lane is itself a filter identity: its SSE frames carry
	// identity cursors that resume only under the same lane.
	assignedPage := doREST(t, fixture.server.Handler(), http.MethodGet, "/v1/feed?wait=0s&scope=assigned", fixture.actorA.Secret, "", nil)
	assertRESTStatus(t, assignedPage, http.StatusOK)
	var lane struct {
		NextCursor string `json:"next_cursor"`
	}
	if err := json.Unmarshal(assignedPage.Body.Bytes(), &lane); err != nil || lane.NextCursor == "" {
		t.Fatalf("no assigned cursor issued: %s", assignedPage.Body.String())
	}
	laneReplay := doREST(t, fixture.server.Handler(), http.MethodGet, "/v1/feed?wait=0s&scope=assigned&cursor="+lane.NextCursor, fixture.actorA.Secret, "", nil)
	assertRESTStatus(t, laneReplay, http.StatusOK)
}
