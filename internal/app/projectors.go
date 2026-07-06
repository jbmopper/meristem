// Package app wires cross-cutting runtime dependencies.
package app

import (
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/convergence"
	"github.com/jbmopper/meristem/internal/errorreporting"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/idempotency"
	"github.com/jbmopper/meristem/internal/inbox"
	"github.com/jbmopper/meristem/internal/jobqueue"
	"github.com/jbmopper/meristem/internal/policyprofile"
	"github.com/jbmopper/meristem/internal/projectiondefs"
	"github.com/jbmopper/meristem/internal/projections"
	"github.com/jbmopper/meristem/internal/registry"
	"github.com/jbmopper/meristem/internal/signals"
	"github.com/jbmopper/meristem/internal/workitems"
)

// NewProjectionRegistry registers every known projector in deterministic
// order. Feature packages own their projector code; the app package owns the
// one place where the running system decides which projections are active.
func NewProjectionRegistry() *projections.Registry {
	reg := projections.NewRegistry()
	auth.RegisterProjectors(reg)
	errorreporting.RegisterProjectors(reg)
	idempotency.RegisterProjectors(reg)
	inbox.RegisterProjectors(reg)
	workitems.RegisterProjectors(reg)
	jobqueue.RegisterProjectors(reg)
	signals.RegisterProjectors(reg)
	policyprofile.RegisterProjectors(reg)
	convergence.RegisterProjectors(reg)
	registry.RegisterProjectors(reg)
	projectiondefs.RegisterProjectors(reg)
	return reg
}

// NewEventWriter returns an event writer backed by the full application
// projector registry.
func NewEventWriter() *events.Writer {
	return events.NewWriter(NewProjectionRegistry())
}
