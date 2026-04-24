// Package app wires cross-cutting runtime dependencies.
package app

import (
	"github.com/jbmopper/wayline/internal/auth"
	"github.com/jbmopper/wayline/internal/events"
	"github.com/jbmopper/wayline/internal/idempotency"
	"github.com/jbmopper/wayline/internal/inbox"
	"github.com/jbmopper/wayline/internal/projections"
	"github.com/jbmopper/wayline/internal/signals"
	"github.com/jbmopper/wayline/internal/workitems"
)

// NewProjectionRegistry registers every known projector in deterministic
// order. Feature packages own their projector code; the app package owns the
// one place where the running system decides which projections are active.
func NewProjectionRegistry() *projections.Registry {
	registry := projections.NewRegistry()
	auth.RegisterProjectors(registry)
	idempotency.RegisterProjectors(registry)
	inbox.RegisterProjectors(registry)
	workitems.RegisterProjectors(registry)
	signals.RegisterProjectors(registry)
	return registry
}

// NewEventWriter returns an event writer backed by the full application
// projector registry.
func NewEventWriter() *events.Writer {
	return events.NewWriter(NewProjectionRegistry())
}
