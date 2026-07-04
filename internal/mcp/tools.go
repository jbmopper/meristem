package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/backlog"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/errorreporting"
	"github.com/jbmopper/meristem/internal/feed"
	"github.com/jbmopper/meristem/internal/policyprofile"
	"github.com/jbmopper/meristem/internal/safety"
	"github.com/jbmopper/meristem/internal/workitems"
)

// Tool is one MCP tool exposed by the server. The handler signature
// matches the spec layout in docs/v0.md (one tool per REST op,
// dot-namespaced) and threads the resolved actor through so every event
// the tool causes is attributed to it.
//
// The handler returns the tool's structured payload; the dispatcher wraps
// it into the MCP content/structuredContent envelope. Returning an error
// means "the tool failed"; the dispatcher turns that into an isError=true
// response, never a transport-level JSON-RPC error.
type Tool struct {
	Name        string
	Description string
	InputSchema map[string]any
	Mutates     bool
	Handler     func(ctx context.Context, actor domain.Token, raw json.RawMessage) (any, error)
}

func (s *Server) buildTools() []Tool {
	tools := []Tool{
		s.toolPolicyProfileSwitch(),
		s.toolInboxCapture(),
		s.toolFeedRead(),
		s.toolBacklogReadiness(),
		s.toolDeterministicErrorsList(),
		s.toolDeterministicErrorsGet(),
		s.toolWorkItemsList(),
		s.toolWorkItemsGet(),
		s.toolWorkItemsCreate(),
		s.toolWorkItemsSpawnChild(),
		s.toolWorkItemsAppendEvent(),
		s.toolWorkItemsUpdateMetadata(),
		s.toolWorkItemsTransition(),
	}
	for i := range tools {
		if tools[i].Mutates {
			tools[i].InputSchema = schemaWithIdempotencyKey(tools[i].InputSchema)
		}
	}
	return tools
}

func (s *Server) toolBacklogReadiness() Tool {
	return Tool{
		Name:        "backlog.readiness",
		Description: "Summarize visible backlog readiness groups from the work_items projection.",
		InputSchema: schemaObject(nil, map[string]any{
			"limit": schemaInt("Max visible work items to classify (0-200). Defaults to 200."),
		}),
		Handler: func(ctx context.Context, actor domain.Token, raw json.RawMessage) (any, error) {
			if s.deps.WorkItems == nil {
				return nil, errors.New("workitems service not configured")
			}
			var args struct {
				Limit int `json:"limit"`
			}
			if err := decodeArgs(raw, &args); err != nil {
				return nil, err
			}
			limit := args.Limit
			if limit == 0 {
				limit = 200
			}
			if limit < 0 || limit > 200 {
				return nil, replayableToolErr(errors.New("limit must be between 0 and 200"))
			}
			items, err := s.deps.WorkItems.List(ctx, "", limit)
			if err != nil {
				return nil, err
			}
			items, err = s.filterWorkItems(ctx, actor, items)
			if err != nil {
				return nil, err
			}
			return backlog.Summarize(items, backlog.Options{
				Limit: limit,
				AsOf:  time.Now().UTC(),
			}), nil
		},
	}
}

func (s *Server) toolInboxCapture() Tool {
	return Tool{
		Name:        "inbox.capture",
		Description: "Capture a text instruction into the inbox; auto-creates a captured work_item.",
		Mutates:     true,
		InputSchema: schemaObject(
			[]string{"text"},
			map[string]any{
				"text": schemaString("Instruction text. Required, non-empty."),
			},
		),
		Handler: func(ctx context.Context, actor domain.Token, raw json.RawMessage) (any, error) {
			if s.deps.Inbox == nil {
				return nil, errors.New("inbox service not configured")
			}
			var args struct {
				Text string `json:"text"`
			}
			if err := decodeArgs(raw, &args); err != nil {
				return nil, err
			}
			if actor.Source != "" && actor.Source != domain.SourceHuman {
				return nil, replayableToolErr(errors.New("inbox.capture requires a human-source token"))
			}
			res, err := s.deps.Inbox.CaptureText(ctx, actor, args.Text)
			if err != nil {
				return nil, err
			}
			return map[string]any{
				"message_id":   res.MessageID,
				"work_item_id": res.WorkItemID,
			}, nil
		},
	}
}

func (s *Server) toolFeedRead() Tool {
	return Tool{
		Name: "feed.read",
		Description: "Read feed-visible events. Default: snapshot (newest first). " +
			"Pass cursor and/or wait (Go duration, e.g. 30s) for watcher mode — same contract as GET /v1/feed (oldest-first page, next_cursor, has_more).",
		InputSchema: schemaObject(nil, map[string]any{
			"limit":  schemaInt("Max items (1-200). Defaults to 50."),
			"cursor": schemaString("Opaque cursor from a prior next_cursor or SSE id. Omit for snapshot mode."),
			"wait":   schemaString("Long-poll cap as a Go duration (e.g. 10s). Use with watcher semantics; server-capped."),
		}),
		Handler: func(ctx context.Context, actor domain.Token, raw json.RawMessage) (any, error) {
			if s.deps.Feed == nil {
				return nil, errors.New("feed service not configured")
			}
			var args struct {
				Limit  int    `json:"limit"`
				Cursor string `json:"cursor"`
				Wait   string `json:"wait"`
			}
			if err := decodeArgs(raw, &args); err != nil {
				return nil, err
			}
			if args.Cursor == "" && args.Wait == "" {
				items, err := s.deps.Feed.List(ctx, args.Limit)
				if err != nil {
					return nil, err
				}
				items, err = s.filterFeedItems(ctx, actor, items)
				if err != nil {
					return nil, err
				}
				return map[string]any{"items": items}, nil
			}
			var wait time.Duration
			if args.Wait != "" {
				parsed, err := time.ParseDuration(args.Wait)
				if err != nil {
					return nil, fmt.Errorf("feed.read: invalid wait: %w", err)
				}
				if parsed < 0 {
					return nil, fmt.Errorf("feed.read: wait must be non-negative")
				}
				wait = parsed
			}
			maxWait := s.deps.MaxFeedWait
			if maxWait == 0 {
				maxWait = safety.DefaultPolicy().MaxFeedWait
			}
			if wait > maxWait {
				return nil, fmt.Errorf("feed.read: wait exceeds server limit (%s)", maxWait)
			}
			page, err := s.deps.Feed.Page(ctx, feed.ListOptions{
				Cursor: args.Cursor,
				Wait:   wait,
				Limit:  args.Limit,
			})
			if err != nil {
				if errors.Is(err, feed.ErrInvalidCursor) {
					return nil, fmt.Errorf("feed.read: invalid_cursor: %w", err)
				}
				return nil, err
			}
			page, err = s.filterFeedPage(ctx, actor, page)
			if err != nil {
				return nil, err
			}
			return map[string]any{
				"items":       page.Items,
				"next_cursor": page.NextCursor,
				"has_more":    page.HasMore,
			}, nil
		},
	}
}

func (s *Server) toolDeterministicErrorsList() Tool {
	return Tool{
		Name:        "deterministic_errors.list",
		Description: "List deterministic log/error records visible to this token. Details are filtered by logs.* scopes.",
		InputSchema: schemaObject(nil, map[string]any{
			"include_masked": schemaBool("Include masked records. Requires logs.read_masked or root."),
			"limit":          schemaInt("Max items to return (1-200). Defaults to 50."),
		}),
		Handler: func(ctx context.Context, actor domain.Token, raw json.RawMessage) (any, error) {
			if s.deps.DeterministicErrors == nil {
				return nil, errors.New("deterministic error service not configured")
			}
			var args struct {
				IncludeMasked bool `json:"include_masked"`
				Limit         int  `json:"limit"`
			}
			if err := decodeArgs(raw, &args); err != nil {
				return nil, err
			}
			items, err := s.deps.DeterministicErrors.ListForAccessor(ctx, errorreporting.ListOptions{
				IncludeMasked: args.IncludeMasked,
				Limit:         args.Limit,
			}, actor)
			if err != nil {
				if errors.Is(err, errorreporting.ErrAccessDenied) {
					return nil, fmt.Errorf("insufficient_scope: token lacks deterministic log visibility scope")
				}
				return nil, err
			}
			out := make([]deterministicErrorDTO, 0, len(items))
			for _, item := range items {
				out = append(out, toDeterministicErrorDTO(item))
			}
			return map[string]any{"items": out}, nil
		},
	}
}

func (s *Server) toolDeterministicErrorsGet() Tool {
	return Tool{
		Name:        "deterministic_errors.get",
		Description: "Fetch one deterministic log/error record visible to this token. Details are filtered by logs.* scopes.",
		InputSchema: schemaObject([]string{"id"}, map[string]any{
			"id": schemaString("Deterministic error uuid."),
		}),
		Handler: func(ctx context.Context, actor domain.Token, raw json.RawMessage) (any, error) {
			if s.deps.DeterministicErrors == nil {
				return nil, errors.New("deterministic error service not configured")
			}
			var args struct {
				ID string `json:"id"`
			}
			if err := decodeArgs(raw, &args); err != nil {
				return nil, err
			}
			id, err := parseUUID(args.ID, "id")
			if err != nil {
				return nil, err
			}
			item, err := s.deps.DeterministicErrors.GetForAccessor(ctx, id, actor)
			if err != nil {
				if errors.Is(err, errorreporting.ErrAccessDenied) {
					return nil, fmt.Errorf("insufficient_scope: token lacks deterministic log visibility scope")
				}
				if errors.Is(err, errorreporting.ErrNotFound) {
					return nil, fmt.Errorf("deterministic error %s not found", id)
				}
				return nil, err
			}
			return map[string]any{"deterministic_error": toDeterministicErrorDTO(item)}, nil
		},
	}
}

func (s *Server) toolWorkItemsList() Tool {
	return Tool{
		Name:        "work_items.list",
		Description: "List work items, newest update first. Optional state filter.",
		InputSchema: schemaObject(nil, map[string]any{
			"state": schemaString("Filter to one lifecycle state (e.g. captured, running, done)."),
			"limit": schemaInt("Max items to return (1-200). Defaults to 50."),
		}),
		Handler: func(ctx context.Context, actor domain.Token, raw json.RawMessage) (any, error) {
			if s.deps.WorkItems == nil {
				return nil, errors.New("workitems service not configured")
			}
			var args struct {
				State string `json:"state"`
				Limit int    `json:"limit"`
			}
			if err := decodeArgs(raw, &args); err != nil {
				return nil, err
			}
			items, err := s.deps.WorkItems.List(ctx, args.State, args.Limit)
			if err != nil {
				return nil, err
			}
			items, err = s.filterWorkItems(ctx, actor, items)
			if err != nil {
				return nil, err
			}
			out := make([]workItemDTO, 0, len(items))
			for _, item := range items {
				out = append(out, toWorkItemDTO(item))
			}
			return map[string]any{"items": out}, nil
		},
	}
}

func (s *Server) toolWorkItemsGet() Tool {
	return Tool{
		Name:        "work_items.get",
		Description: "Fetch a single work item by id.",
		InputSchema: schemaObject([]string{"id"}, map[string]any{
			"id": schemaString("Work item uuid."),
		}),
		Handler: func(ctx context.Context, actor domain.Token, raw json.RawMessage) (any, error) {
			if s.deps.WorkItems == nil {
				return nil, errors.New("workitems service not configured")
			}
			var args struct {
				ID string `json:"id"`
			}
			if err := decodeArgs(raw, &args); err != nil {
				return nil, err
			}
			id, err := parseUUID(args.ID, "id")
			if err != nil {
				return nil, err
			}
			if err := s.canReadWorkItem(ctx, actor, id); err != nil {
				return nil, err
			}
			item, err := s.deps.WorkItems.Get(ctx, id)
			if err != nil {
				if errors.Is(err, workitems.ErrNotFound) {
					return nil, fmt.Errorf("work item %s not found", id)
				}
				return nil, err
			}
			return map[string]any{"work_item": toWorkItemDTO(item)}, nil
		},
	}
}

func (s *Server) toolWorkItemsCreate() Tool {
	return Tool{
		Name:        "work_items.create",
		Description: "Create a new top-level work item.",
		Mutates:     true,
		InputSchema: schemaObject([]string{"title"}, map[string]any{
			"title": schemaString("Short title. Required, non-empty."),
			"body":  schemaString("Optional long-form body."),
			"state": schemaString("Optional initial lifecycle state. Defaults to captured."),
			"suggested_convergence_checks": schemaStringArray(
				"Optional list of deterministic checks a worker should satisfy before claiming convergence.",
			),
			"human_review_status": schemaString(
				"Optional human review status (blocked, waved_through, approved). Defaults to waved_through.",
			),
		}),
		Handler: func(ctx context.Context, actor domain.Token, raw json.RawMessage) (any, error) {
			if s.deps.WorkItems == nil {
				return nil, errors.New("workitems service not configured")
			}
			var args struct {
				Title                      string   `json:"title"`
				Body                       string   `json:"body"`
				State                      string   `json:"state"`
				SuggestedConvergenceChecks []string `json:"suggested_convergence_checks"`
				HumanReviewStatus          string   `json:"human_review_status"`
			}
			if err := decodeArgs(raw, &args); err != nil {
				return nil, err
			}
			if err := validateWorkItemCreateArgs(args.Title, args.State, args.SuggestedConvergenceChecks, args.HumanReviewStatus); err != nil {
				return nil, err
			}
			if err := s.canCreateWorkItem(ctx, actor); err != nil {
				return nil, err
			}
			item, err := s.deps.WorkItems.Create(ctx, workitems.CreateInput{
				Title:                      args.Title,
				Body:                       args.Body,
				State:                      domain.WorkItemState(args.State),
				SuggestedConvergenceChecks: args.SuggestedConvergenceChecks,
				HumanReviewStatus:          domain.HumanReviewStatus(args.HumanReviewStatus),
				Actor:                      actor,
			})
			if err != nil {
				return nil, err
			}
			return map[string]any{
				"work_item_id": item.ID,
				"work_item":    toWorkItemDTO(item),
			}, nil
		},
	}
}

func (s *Server) toolWorkItemsSpawnChild() Tool {
	return Tool{
		Name:        "work_items.spawn_child",
		Description: "Create a child work item under an existing parent.",
		Mutates:     true,
		InputSchema: schemaObject([]string{"parent_id", "title"}, map[string]any{
			"parent_id": schemaString("Parent work item uuid."),
			"title":     schemaString("Short title. Required, non-empty."),
			"body":      schemaString("Optional long-form body."),
			"state":     schemaString("Optional initial lifecycle state. Defaults to captured."),
			"suggested_convergence_checks": schemaStringArray(
				"Optional list of deterministic checks a worker should satisfy before claiming convergence.",
			),
			"human_review_status": schemaString(
				"Optional human review status (blocked, waved_through, approved). Defaults to waved_through.",
			),
		}),
		Handler: func(ctx context.Context, actor domain.Token, raw json.RawMessage) (any, error) {
			if s.deps.WorkItems == nil {
				return nil, errors.New("workitems service not configured")
			}
			var args struct {
				ParentID                   string   `json:"parent_id"`
				Title                      string   `json:"title"`
				Body                       string   `json:"body"`
				State                      string   `json:"state"`
				SuggestedConvergenceChecks []string `json:"suggested_convergence_checks"`
				HumanReviewStatus          string   `json:"human_review_status"`
			}
			if err := decodeArgs(raw, &args); err != nil {
				return nil, err
			}
			if err := validateWorkItemCreateArgs(args.Title, args.State, args.SuggestedConvergenceChecks, args.HumanReviewStatus); err != nil {
				return nil, err
			}
			parent, err := parseUUID(args.ParentID, "parent_id")
			if err != nil {
				return nil, err
			}
			if err := s.canWriteWorkItem(ctx, actor, parent); err != nil {
				return nil, err
			}
			item, err := s.deps.WorkItems.SpawnChild(ctx, parent, workitems.CreateInput{
				Title:                      args.Title,
				Body:                       args.Body,
				State:                      domain.WorkItemState(args.State),
				SuggestedConvergenceChecks: args.SuggestedConvergenceChecks,
				HumanReviewStatus:          domain.HumanReviewStatus(args.HumanReviewStatus),
				Actor:                      actor,
			})
			if err != nil {
				if errors.Is(err, workitems.ErrNotFound) {
					return nil, replayableToolErr(fmt.Errorf("parent work item %s not found", parent))
				}
				if errors.Is(err, workitems.ErrRelationCycle) {
					return nil, replayableToolErr(fmt.Errorf("relation cycle: parent %s descends from child", parent))
				}
				return nil, err
			}
			return map[string]any{
				"parent_id":    parent,
				"work_item_id": item.ID,
				"work_item":    toWorkItemDTO(item),
			}, nil
		},
	}
}

func (s *Server) toolWorkItemsAppendEvent() Tool {
	return Tool{
		Name:        "work_items.append_event",
		Description: "Append a free-form progress event to a work item.",
		Mutates:     true,
		InputSchema: schemaObject([]string{"id", "kind"}, map[string]any{
			"id":      schemaString("Work item uuid."),
			"kind":    schemaString("Inner event kind (e.g. agent.tool_used). Required."),
			"payload": schemaAny("Arbitrary JSON payload describing the event."),
		}),
		Handler: func(ctx context.Context, actor domain.Token, raw json.RawMessage) (any, error) {
			if s.deps.WorkItems == nil {
				return nil, errors.New("workitems service not configured")
			}
			var args struct {
				ID      string          `json:"id"`
				Kind    string          `json:"kind"`
				Payload json.RawMessage `json:"payload"`
			}
			if err := decodeArgs(raw, &args); err != nil {
				return nil, err
			}
			id, err := parseUUID(args.ID, "id")
			if err != nil {
				return nil, err
			}
			if strings.TrimSpace(args.Kind) == "" {
				return nil, replayableToolErr(errors.New("workitems: event kind is required"))
			}
			if err := s.canWriteWorkItem(ctx, actor, id); err != nil {
				return nil, err
			}
			var payload any
			if len(args.Payload) > 0 {
				if err := json.Unmarshal(args.Payload, &payload); err != nil {
					return nil, replayableToolErr(fmt.Errorf("payload: %w", err))
				}
			}
			if err := s.deps.WorkItems.AppendEvent(ctx, id, args.Kind, payload, actor); err != nil {
				if errors.Is(err, workitems.ErrNotFound) {
					return nil, replayableToolErr(fmt.Errorf("work item %s not found", id))
				}
				return nil, err
			}
			return map[string]any{"work_item_id": id, "appended": true}, nil
		},
	}
}

func (s *Server) toolWorkItemsUpdateMetadata() Tool {
	return Tool{
		Name:        "work_items.update_metadata",
		Description: "Set suggested convergence checks and human review status on a work item.",
		Mutates:     true,
		InputSchema: schemaObject([]string{"id", "suggested_convergence_checks", "human_review_status"}, map[string]any{
			"id": schemaString("Work item uuid."),
			"suggested_convergence_checks": schemaStringArray(
				"Complete list of deterministic checks a worker should satisfy before claiming convergence.",
			),
			"human_review_status": schemaString(
				"Human review status: blocked, waved_through, or approved.",
			),
		}),
		Handler: func(ctx context.Context, actor domain.Token, raw json.RawMessage) (any, error) {
			if s.deps.WorkItems == nil {
				return nil, errors.New("workitems service not configured")
			}
			var args struct {
				ID                         string   `json:"id"`
				SuggestedConvergenceChecks []string `json:"suggested_convergence_checks"`
				HumanReviewStatus          string   `json:"human_review_status"`
			}
			if err := decodeArgs(raw, &args); err != nil {
				return nil, err
			}
			id, err := parseUUID(args.ID, "id")
			if err != nil {
				return nil, err
			}
			if err := validateWorkItemMetadataArgs(args.SuggestedConvergenceChecks, args.HumanReviewStatus); err != nil {
				return nil, err
			}
			if err := s.canWriteWorkItem(ctx, actor, id); err != nil {
				return nil, err
			}
			item, err := s.deps.WorkItems.UpdateMetadata(ctx, id, workitems.UpdateMetadataInput{
				SuggestedConvergenceChecks: args.SuggestedConvergenceChecks,
				HumanReviewStatus:          domain.HumanReviewStatus(args.HumanReviewStatus),
				Actor:                      actor,
			})
			if err != nil {
				if errors.Is(err, workitems.ErrNotFound) {
					return nil, replayableToolErr(fmt.Errorf("work item %s not found", id))
				}
				return nil, err
			}
			return map[string]any{"work_item": toWorkItemDTO(item)}, nil
		},
	}
}

func (s *Server) toolWorkItemsTransition() Tool {
	return Tool{
		Name:        "work_items.transition",
		Description: "Move a work item to another lifecycle state.",
		Mutates:     true,
		InputSchema: schemaObject([]string{"id", "to"}, map[string]any{
			"id":     schemaString("Work item uuid."),
			"to":     schemaString("Target lifecycle state (captured, triaged, planned, awaiting_approval, running, blocked, done, failed, canceled)."),
			"reason": schemaString("Optional reason recorded with the transition."),
		}),
		Handler: func(ctx context.Context, actor domain.Token, raw json.RawMessage) (any, error) {
			if s.deps.WorkItems == nil {
				return nil, errors.New("workitems service not configured")
			}
			var args struct {
				ID     string `json:"id"`
				To     string `json:"to"`
				Reason string `json:"reason"`
			}
			if err := decodeArgs(raw, &args); err != nil {
				return nil, err
			}
			id, err := parseUUID(args.ID, "id")
			if err != nil {
				return nil, err
			}
			to := domain.WorkItemState(args.To)
			if !to.Valid() {
				return nil, replayableToolErr(fmt.Errorf("workitems: invalid state %q", to))
			}
			if err := s.canWriteWorkItem(ctx, actor, id); err != nil {
				return nil, err
			}
			item, err := s.deps.WorkItems.Transition(ctx, id, to, args.Reason, actor)
			if err != nil {
				if errors.Is(err, workitems.ErrNotFound) {
					return nil, replayableToolErr(fmt.Errorf("work item %s not found", id))
				}
				if strings.Contains(err.Error(), "invalid transition") {
					return nil, replayableToolErr(err)
				}
				return nil, err
			}
			return map[string]any{"work_item": toWorkItemDTO(item)}, nil
		},
	}
}

func (s *Server) filterFeedItems(ctx context.Context, actor domain.Token, items []feed.Item) ([]feed.Item, error) {
	if s.deps.Access == nil {
		if access.RequiresScopedPolicy(actor) {
			return nil, errors.New("access service not configured")
		}
		return items, nil
	}
	filtered, err := s.deps.Access.FilterFeedItems(ctx, actor, items)
	if errors.Is(err, access.ErrDenied) {
		return nil, replayableToolErr(fmt.Errorf("insufficient_scope: token cannot read feed"))
	}
	return filtered, err
}

func (s *Server) filterFeedPage(ctx context.Context, actor domain.Token, page feed.Page) (feed.Page, error) {
	if s.deps.Access == nil {
		if access.RequiresScopedPolicy(actor) {
			return feed.Page{}, errors.New("access service not configured")
		}
		return page, nil
	}
	filtered, err := s.deps.Access.FilterFeedPage(ctx, actor, page)
	if errors.Is(err, access.ErrDenied) {
		return feed.Page{}, replayableToolErr(fmt.Errorf("insufficient_scope: token cannot read feed"))
	}
	return filtered, err
}

func (s *Server) filterWorkItems(ctx context.Context, actor domain.Token, items []domain.WorkItem) ([]domain.WorkItem, error) {
	if s.deps.Access == nil {
		if access.RequiresScopedPolicy(actor) {
			return nil, errors.New("access service not configured")
		}
		return items, nil
	}
	filtered, err := s.deps.Access.FilterWorkItems(ctx, actor, items)
	if errors.Is(err, access.ErrDenied) {
		return nil, replayableToolErr(fmt.Errorf("insufficient_scope: token cannot read work_items"))
	}
	return filtered, err
}

func (s *Server) canReadWorkItem(ctx context.Context, actor domain.Token, id uuid.UUID) error {
	if s.deps.Access == nil {
		if access.RequiresScopedPolicy(actor) {
			return errors.New("access service not configured")
		}
		return nil
	}
	if err := s.deps.Access.CanReadWorkItem(ctx, actor, id); err != nil {
		if errors.Is(err, access.ErrDenied) {
			return replayableToolErr(fmt.Errorf("work item %s not found", id))
		}
		return err
	}
	return nil
}

func (s *Server) canCreateWorkItem(ctx context.Context, actor domain.Token) error {
	if s.deps.Access == nil {
		if access.RequiresScopedPolicy(actor) {
			return errors.New("access service not configured")
		}
		return nil
	}
	if err := s.deps.Access.CanCreateWorkItem(ctx, actor); err != nil {
		if errors.Is(err, access.ErrDenied) {
			return replayableToolErr(fmt.Errorf("insufficient_scope: token cannot create top-level work_items"))
		}
		return err
	}
	return nil
}

func (s *Server) canWriteWorkItem(ctx context.Context, actor domain.Token, id uuid.UUID) error {
	if s.deps.Access == nil {
		if access.RequiresScopedPolicy(actor) {
			return errors.New("access service not configured")
		}
		return nil
	}
	if err := s.deps.Access.CanWriteWorkItem(ctx, actor, id); err != nil {
		if errors.Is(err, access.ErrDenied) {
			return replayableToolErr(fmt.Errorf("work item %s not found", id))
		}
		return err
	}
	return nil
}

// workItemDTO is the JSON shape returned by tools. It mirrors the HTTP
// response so MCP clients see the same fields as REST clients.
type workItemDTO struct {
	ID                         uuid.UUID                `json:"id"`
	Title                      string                   `json:"title"`
	Body                       string                   `json:"body"`
	State                      domain.WorkItemState     `json:"state"`
	StateReason                *string                  `json:"state_reason,omitempty"`
	SuggestedConvergenceChecks []string                 `json:"suggested_convergence_checks"`
	HumanReviewStatus          domain.HumanReviewStatus `json:"human_review_status"`
	CreatedBy                  *uuid.UUID               `json:"created_by,omitempty"`
	CreatedAt                  string                   `json:"created_at"`
	StateEnteredAt             string                   `json:"state_entered_at"`
	UpdatedAt                  string                   `json:"updated_at"`
}

type deterministicErrorDTO struct {
	ID         uuid.UUID                         `json:"id"`
	Component  string                            `json:"component"`
	Code       string                            `json:"code"`
	Message    string                            `json:"message"`
	Severity   domain.DeterministicErrorSeverity `json:"severity"`
	Details    json.RawMessage                   `json:"details"`
	ReportedBy *uuid.UUID                        `json:"reported_by,omitempty"`
	ReportedAt string                            `json:"reported_at"`
	UpdatedAt  string                            `json:"updated_at"`
	Masked     bool                              `json:"masked"`
	MaskReason *string                           `json:"mask_reason,omitempty"`
	MaskedBy   *uuid.UUID                        `json:"masked_by,omitempty"`
	MaskedAt   *string                           `json:"masked_at,omitempty"`
}

func toDeterministicErrorDTO(item domain.DeterministicError) deterministicErrorDTO {
	details := json.RawMessage(item.Details)
	if len(details) == 0 {
		details = json.RawMessage(`{}`)
	}
	var maskedAt *string
	if item.MaskedAt != nil {
		formatted := item.MaskedAt.UTC().Format("2006-01-02T15:04:05.999999999Z07:00")
		maskedAt = &formatted
	}
	return deterministicErrorDTO{
		ID:         item.ID,
		Component:  item.Component,
		Code:       item.Code,
		Message:    item.Message,
		Severity:   item.Severity,
		Details:    details,
		ReportedBy: item.ReportedBy,
		ReportedAt: item.ReportedAt.UTC().Format("2006-01-02T15:04:05.999999999Z07:00"),
		UpdatedAt:  item.UpdatedAt.UTC().Format("2006-01-02T15:04:05.999999999Z07:00"),
		Masked:     item.Masked,
		MaskReason: item.MaskReason,
		MaskedBy:   item.MaskedBy,
		MaskedAt:   maskedAt,
	}
}

func toWorkItemDTO(item domain.WorkItem) workItemDTO {
	return workItemDTO{
		ID:                         item.ID,
		Title:                      item.Title,
		Body:                       item.Body,
		State:                      item.State,
		StateReason:                item.StateReason,
		SuggestedConvergenceChecks: item.SuggestedConvergenceChecks,
		HumanReviewStatus:          item.HumanReviewStatus,
		CreatedBy:                  item.CreatedBy,
		CreatedAt:                  item.CreatedAt.UTC().Format("2006-01-02T15:04:05.999999999Z07:00"),
		StateEnteredAt:             item.StateEnteredAt.UTC().Format("2006-01-02T15:04:05.999999999Z07:00"),
		UpdatedAt:                  item.UpdatedAt.UTC().Format("2006-01-02T15:04:05.999999999Z07:00"),
	}
}

func decodeArgs(raw json.RawMessage, out any) error {
	if len(raw) == 0 {
		return nil
	}
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil {
		return fmt.Errorf("invalid arguments: %w", err)
	}
	return nil
}

func parseUUID(raw, field string) (uuid.UUID, error) {
	id, err := uuid.Parse(strings.TrimSpace(raw))
	if err != nil {
		return uuid.Nil, fmt.Errorf("%s must be a valid uuid", field)
	}
	return id, nil
}

func validateWorkItemCreateArgs(title, state string, checks []string, humanReview string) error {
	if strings.TrimSpace(title) == "" {
		return replayableToolErr(errors.New("workitems: title is required"))
	}
	if err := validateWorkItemStateArg(state); err != nil {
		return err
	}
	return validateWorkItemMetadataArgs(checks, humanReview)
}

func validateWorkItemMetadataArgs(checks []string, humanReview string) error {
	for i, check := range checks {
		if strings.TrimSpace(check) == "" {
			return replayableToolErr(fmt.Errorf("workitems: suggested_convergence_checks[%d] is blank", i))
		}
	}
	if humanReview != "" && !domain.HumanReviewStatus(humanReview).Valid() {
		return replayableToolErr(fmt.Errorf("workitems: invalid human_review_status %q", humanReview))
	}
	return nil
}

func validateWorkItemStateArg(state string) error {
	if state == "" {
		return nil
	}
	parsed := domain.WorkItemState(state)
	if !parsed.Valid() {
		return replayableToolErr(fmt.Errorf("workitems: invalid state %q", parsed))
	}
	return nil
}

func schemaObject(required []string, properties map[string]any) map[string]any {
	out := map[string]any{
		"type":                 "object",
		"properties":           properties,
		"additionalProperties": false,
	}
	if len(required) > 0 {
		out["required"] = required
	}
	return out
}

func schemaWithIdempotencyKey(schema map[string]any) map[string]any {
	out := make(map[string]any, len(schema))
	for key, value := range schema {
		out[key] = value
	}
	props, _ := out["properties"].(map[string]any)
	if props == nil {
		props = make(map[string]any)
	} else {
		copied := make(map[string]any, len(props)+1)
		for key, value := range props {
			copied[key] = value
		}
		props = copied
	}
	props["idempotency_key"] = schemaString("Required idempotency key for MCP mutation calls. Reuse only for the same tool arguments.")
	out["properties"] = props

	required, _ := out["required"].([]string)
	for _, field := range required {
		if field == "idempotency_key" {
			return out
		}
	}
	copiedRequired := append([]string{}, required...)
	copiedRequired = append(copiedRequired, "idempotency_key")
	out["required"] = copiedRequired
	return out
}

func schemaString(description string) map[string]any {
	return map[string]any{"type": "string", "description": description}
}

func schemaStringArray(description string) map[string]any {
	return map[string]any{
		"type":        "array",
		"description": description,
		"items":       map[string]any{"type": "string"},
	}
}

func schemaInt(description string) map[string]any {
	return map[string]any{"type": "integer", "description": description}
}

func schemaBool(description string) map[string]any {
	return map[string]any{"type": "boolean", "description": description}
}

func schemaAny(description string) map[string]any {
	return map[string]any{"description": description}
}

func (s *Server) toolPolicyProfileSwitch() Tool {
	return Tool{
		Name:        "policy_profile.switch",
		Description: "Switch the active safety policy profile (bring-up or steady). Human, non-root tokens only.",
		Mutates:     true,
		InputSchema: schemaObject([]string{"profile"}, map[string]any{
			"profile": schemaString("Target profile name: bring-up or steady."),
		}),
		Handler: func(ctx context.Context, actor domain.Token, raw json.RawMessage) (any, error) {
			if s.deps.PolicyProfiles == nil {
				return nil, errors.New("policy profile service not configured")
			}
			var args struct {
				Profile string `json:"profile"`
			}
			if err := decodeArgs(raw, &args); err != nil {
				return nil, err
			}
			if args.Profile == "" {
				return nil, replayableToolErr(errors.New("profile_required: profile is required"))
			}
			if _, err := safety.ProfileByName(args.Profile); err != nil {
				return nil, replayableToolErr(err)
			}
			active, switched, err := s.deps.PolicyProfiles.Switch(ctx, policyprofile.SwitchInput{
				To:    args.Profile,
				Actor: actor,
			})
			if err != nil {
				return nil, err
			}
			return map[string]any{
				"profile":     active.Name,
				"fingerprint": active.Fingerprint,
				"switched":    switched,
			}, nil
		},
	}
}
