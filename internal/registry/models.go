// Package registry owns R2 tropism and cultivar registry state.
//
// Tropisms and cultivars are event-sourced: tropism.defined and
// cultivar.defined events are truth, while the tropisms/cultivars tables are
// current-state projections used for reads and validation.
package registry

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type ReducerRef struct {
	Identity string `json:"identity"`
	Version  int    `json:"version"`
}

type TropismRef struct {
	Name    string `json:"name"`
	Version int    `json:"version"`
}

type Profile struct {
	Briefing       string   `json:"briefing"`
	ScopesTemplate []string `json:"scopes_template"`
}

type Xylem struct {
	MaxAttempts                    int            `json:"max_attempts"`
	MaxWallSeconds                 int            `json:"max_wall_seconds"`
	MaxDepth                       int            `json:"max_depth"`
	MaxChildrenPerItem             int            `json:"max_children_per_item,omitempty"`
	MaxConcurrentRunningPerToken   int            `json:"max_concurrent_running_items_per_token,omitempty"`
	MaxEventsPerItemPerHourByClass map[string]int `json:"max_events_per_item_per_hour_by_class,omitempty"`
}

type Tropism struct {
	Name        string          `json:"name"`
	Version     int             `json:"version"`
	Reducer     ReducerRef      `json:"reducer"`
	Params      json.RawMessage `json:"params"`
	Description string          `json:"description"`
	EventID     uuid.UUID       `json:"event_id"`
	DefinedAt   time.Time       `json:"defined_at"`
	DefinedBy   *uuid.UUID      `json:"defined_by,omitempty"`
	Source      string          `json:"source"`
}

type Cultivar struct {
	Name        string     `json:"name"`
	Version     int        `json:"version"`
	Rootstock   bool       `json:"rootstock"`
	Tropism     TropismRef `json:"tropism"`
	Profile     Profile    `json:"profile"`
	Xylem       Xylem      `json:"xylem"`
	Phloem      string     `json:"phloem"`
	Description string     `json:"description"`
	EventID     uuid.UUID  `json:"event_id"`
	DefinedAt   time.Time  `json:"defined_at"`
	DefinedBy   *uuid.UUID `json:"defined_by,omitempty"`
	Source      string     `json:"source"`
}

type Snapshot struct {
	Tropisms  []Tropism  `json:"tropisms"`
	Cultivars []Cultivar `json:"cultivars"`
}

type DefineTropismInput struct {
	Name        string          `json:"name"`
	Version     int             `json:"version"`
	Reducer     ReducerRef      `json:"reducer"`
	Params      json.RawMessage `json:"params"`
	Description string          `json:"description"`
}

type DefineCultivarInput struct {
	Name        string     `json:"name"`
	Version     int        `json:"version"`
	Rootstock   bool       `json:"rootstock"`
	Tropism     TropismRef `json:"tropism"`
	Profile     Profile    `json:"profile"`
	Xylem       Xylem      `json:"xylem"`
	Phloem      string     `json:"phloem"`
	Description string     `json:"description"`
}
