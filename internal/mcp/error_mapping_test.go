package mcp

import (
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/jbmopper/meristem/internal/approvals"
	"github.com/jbmopper/meristem/internal/httpconnector"
	"github.com/jbmopper/meristem/internal/projectiondefs"
	"github.com/jbmopper/meristem/internal/registry"
	"github.com/jbmopper/meristem/internal/workitems"
)

func TestMCPToolErrorMappersUseTypedDomainErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{"work item not found", workItemToolErr(workitems.ErrNotFound, nil)},
		{"work item invalid state", workItemToolErr(fmt.Errorf("%w: bogus", workitems.ErrInvalidState), nil)},
		{"work item invalid transition", workItemToolErr(fmt.Errorf("%w: from done to running", workitems.ErrInvalidTransition), nil)},
		{"work item validation", workItemToolErr(fmt.Errorf("%w: title is required", workitems.ErrInvalidRequest), nil)},
		{"xylem budget", workItemToolErr(fmt.Errorf("%w: max_children_per_item", workitems.ErrXylemBudgetExhausted), nil)},
		{"registry conflict", registryToolErr(fmt.Errorf("%w: cultivar already exists", registry.ErrVersionConflict))},
		{"projection conflict", projectionToolErr(fmt.Errorf("%w: projection already exists", projectiondefs.ErrVersionConflict))},
		{"approval validation", approvalToolErr(fmt.Errorf("%w: summary is required", approvals.ErrInvalidRequest))},
		{"connector approval required", httpConnectorToolErr(httpconnector.ErrApprovalRequired)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !isReplayableToolError(tt.err) {
				t.Fatalf("mapped error is not replayable: %T %v", tt.err, tt.err)
			}
			if got := mutationToolErrorStatus(tt.err); got != http.StatusOK {
				t.Fatalf("mutation status = %d, want 200", got)
			}
		})
	}
}

func TestMCPToolErrorMappersLeaveInfrastructureErrorsUnwrapped(t *testing.T) {
	err := errors.New("pgx: prepared statement not found")
	for name, got := range map[string]error{
		"work_item":  workItemToolErr(err, nil),
		"registry":   registryToolErr(err),
		"projection": projectionToolErr(err),
		"approval":   approvalToolErr(err),
		"connector":  httpConnectorToolErr(err),
	} {
		t.Run(name, func(t *testing.T) {
			if got != err {
				t.Fatalf("infra error was remapped: %T %v", got, got)
			}
			if status := mutationToolErrorStatus(got); status != http.StatusInternalServerError {
				t.Fatalf("infra status = %d, want 500", status)
			}
		})
	}
}
