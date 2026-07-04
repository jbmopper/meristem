package projectiondefs

import (
	"time"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/feed"
)

type Projection struct {
	Name        string                `json:"name"`
	Version     int                   `json:"version"`
	Type        string                `json:"type"`
	Rootstock   bool                  `json:"rootstock"`
	Filter      feed.ProjectionFilter `json:"filter"`
	Description string                `json:"description"`
	EventID     uuid.UUID             `json:"event_id"`
	DefinedAt   time.Time             `json:"defined_at"`
	DefinedBy   *uuid.UUID            `json:"defined_by,omitempty"`
	Source      string                `json:"source"`
}

type Snapshot struct {
	Projections []Projection `json:"projections"`
}

type DefineInput struct {
	Name        string                `json:"name"`
	Version     int                   `json:"version"`
	Type        string                `json:"type"`
	Rootstock   bool                  `json:"rootstock"`
	Filter      feed.ProjectionFilter `json:"filter"`
	Description string                `json:"description"`
}
