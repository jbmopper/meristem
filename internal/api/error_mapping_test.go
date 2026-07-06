package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/approvals"
	"github.com/jbmopper/meristem/internal/grants"
	"github.com/jbmopper/meristem/internal/projectiondefs"
	"github.com/jbmopper/meristem/internal/registry"
	"github.com/jbmopper/meristem/internal/workitems"
)

func TestAPIErrorMappersUseTypedDomainErrors(t *testing.T) {
	tests := []struct {
		name   string
		write  func(http.ResponseWriter)
		status int
		code   string
	}{
		{
			name: "work item not found",
			write: func(w http.ResponseWriter) {
				writeWorkItemError(w, workitems.ErrNotFound)
			},
			status: http.StatusNotFound,
			code:   "work_item_not_found",
		},
		{
			name: "work item invalid state",
			write: func(w http.ResponseWriter) {
				writeWorkItemError(w, fmt.Errorf("%w: bogus", workitems.ErrInvalidState))
			},
			status: http.StatusBadRequest,
			code:   "invalid_state",
		},
		{
			name: "work item invalid transition",
			write: func(w http.ResponseWriter) {
				writeWorkItemError(w, fmt.Errorf("%w: from done to running", workitems.ErrInvalidTransition))
			},
			status: http.StatusConflict,
			code:   "invalid_transition",
		},
		{
			name: "work item validation",
			write: func(w http.ResponseWriter) {
				writeWorkItemError(w, fmt.Errorf("%w: title is required", workitems.ErrInvalidRequest))
			},
			status: http.StatusBadRequest,
			code:   "work_item_request_failed",
		},
		{
			name: "xylem budget",
			write: func(w http.ResponseWriter) {
				writeWorkItemError(w, fmt.Errorf("%w: max_children_per_item", workitems.ErrXylemBudgetExhausted))
			},
			status: http.StatusConflict,
			code:   "xylem_budget_exhausted",
		},
		{
			name: "unexpected event dedupe",
			write: func(w http.ResponseWriter) {
				writeWorkItemError(w, fmt.Errorf("%w: kind=work_item.transitioned", workitems.ErrUnexpectedEventDedupe))
			},
			status: http.StatusConflict,
			code:   "unexpected_event_dedupe",
		},
		{
			name: "access denied",
			write: func(w http.ResponseWriter) {
				writeAccessError(w, access.ErrDenied, "token cannot read work_items")
			},
			status: http.StatusForbidden,
			code:   "insufficient_scope",
		},
		{
			name: "approval invalid request",
			write: func(w http.ResponseWriter) {
				writeApprovalError(w, fmt.Errorf("%w: summary is required", approvals.ErrInvalidRequest))
			},
			status: http.StatusBadRequest,
			code:   "approval_request_failed",
		},
		{
			name: "grant invalid request",
			write: func(w http.ResponseWriter) {
				writeGrantError(w, fmt.Errorf("%w: requested_scopes[0] is blank", grants.ErrInvalidRequest))
			},
			status: http.StatusBadRequest,
			code:   "subactor_grant_request_failed",
		},
		{
			name: "registry conflict",
			write: func(w http.ResponseWriter) {
				writeRegistryError(w, fmt.Errorf("%w: cultivar already exists", registry.ErrVersionConflict))
			},
			status: http.StatusConflict,
			code:   "version_conflict",
		},
		{
			name: "projection conflict",
			write: func(w http.ResponseWriter) {
				writeProjectionError(w, fmt.Errorf("%w: projection already exists", projectiondefs.ErrVersionConflict))
			},
			status: http.StatusConflict,
			code:   "version_conflict",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			tt.write(rec)
			if rec.Code != tt.status {
				t.Fatalf("status = %d, want %d body=%s", rec.Code, tt.status, rec.Body.String())
			}
			var body struct {
				Error struct {
					Code string `json:"code"`
				} `json:"error"`
			}
			if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
				t.Fatalf("decode error body: %v body=%s", err, rec.Body.String())
			}
			if body.Error.Code != tt.code {
				t.Fatalf("code = %q, want %q body=%s", body.Error.Code, tt.code, rec.Body.String())
			}
		})
	}
}
