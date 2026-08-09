// Package app wires cross-cutting runtime dependencies.
package app

import (
	"github.com/jbmopper/meristem/internal/approvals"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/buildguard"
	"github.com/jbmopper/meristem/internal/convergence"
	"github.com/jbmopper/meristem/internal/crossnode"
	"github.com/jbmopper/meristem/internal/errorreporting"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/httpconnector"
	"github.com/jbmopper/meristem/internal/idempotency"
	"github.com/jbmopper/meristem/internal/inbox"
	"github.com/jbmopper/meristem/internal/jobqueue"
	"github.com/jbmopper/meristem/internal/listeneractivation"
	"github.com/jbmopper/meristem/internal/listeners"
	"github.com/jbmopper/meristem/internal/nodes"
	"github.com/jbmopper/meristem/internal/oauth"
	"github.com/jbmopper/meristem/internal/policyprofile"
	"github.com/jbmopper/meristem/internal/projectiondefs"
	"github.com/jbmopper/meristem/internal/projections"
	"github.com/jbmopper/meristem/internal/registry"
	"github.com/jbmopper/meristem/internal/signals"
	"github.com/jbmopper/meristem/internal/spoke"
	"github.com/jbmopper/meristem/internal/workitems"
)

// NewProjectionRegistry registers every known projector in deterministic
// order. Feature packages own their projector code; the app package owns the
// one place where the running system decides which projections are active.
func NewProjectionRegistry() *projections.Registry {
	reg := projections.NewRegistry()
	auth.RegisterProjectors(reg)
	approvals.RegisterProjectors(reg)
	errorreporting.RegisterProjectors(reg)
	idempotency.RegisterProjectors(reg)
	inbox.RegisterProjectors(reg)
	workitems.RegisterProjectors(reg)
	listeners.RegisterProjectors(reg)
	listeneractivation.RegisterProjectors(reg)
	jobqueue.RegisterProjectors(reg)
	httpconnector.RegisterProjectors(reg)
	signals.RegisterProjectors(reg)
	policyprofile.RegisterProjectors(reg)
	convergence.RegisterProjectors(reg)
	registry.RegisterProjectors(reg)
	projectiondefs.RegisterProjectors(reg)
	nodes.RegisterProjectors(reg)
	crossnode.RegisterProjectors(reg)
	spoke.RegisterProjectors(reg)
	oauth.RegisterProjectors(reg)
	return reg
}

// NewEventWriter returns an event writer backed by the full application
// projector registry.
func NewEventWriter() *events.Writer {
	return events.NewWriter(NewProjectionRegistry())
}

// NewGuardedEventWriter returns the application writer with a dynamic
// reviewed-build check immediately before every append. Boundary checks make
// failures legible to API/MCP clients; this hook narrows the interval between a
// transport check and the authoritative event insert if the shared v1 pin
// moves while a request is in flight. It does not revoke a transaction that is
// already committing; deployment quiescence is a separate operator concern.
func NewGuardedEventWriter(guard buildguard.StatusProvider) *events.Writer {
	return events.NewWriterWithPreAppend(NewProjectionRegistry(), func() error {
		return buildguard.RequireNonBlocking(guard)
	})
}
