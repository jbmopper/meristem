// Package app wires cross-cutting runtime dependencies.
package app

import (
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/convergence"
	"github.com/jbmopper/meristem/internal/errorreporting"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/idempotency"
	"github.com/jbmopper/meristem/internal/inbox"
	"github.com/jbmopper/meristem/internal/policyprofile"
	"github.com/jbmopper/meristem/internal/projections"
	"github.com/jbmopper/meristem/internal/signals"
	"github.com/jbmopper/meristem/internal/workitems"
)

// NewProjectionRegistry registers every known projector in deterministic
// order. Feature packages own their projector code; the app package owns the
// one place where the running system decides which projections are active.
func NewProjectionRegistry() *projections.Registry {
	registry := projections.NewRegistry()
	auth.RegisterProjectors(registry)
	errorreporting.RegisterProjectors(registry)
	idempotency.RegisterProjectors(registry)
	inbox.RegisterProjectors(registry)
	workitems.RegisterProjectors(registry)
	signals.RegisterProjectors(registry)
	policyprofile.RegisterProjectors(registry)
	convergence.RegisterProjectors(registry)
	return registry
}

// NewEventWriter returns an event writer backed by the full application
// projector registry.
func NewEventWriter() *events.Writer {
	return events.NewWriter(NewProjectionRegistry())
}
