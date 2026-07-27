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
	"github.com/jbmopper/meristem/internal/approvals"
	"github.com/jbmopper/meristem/internal/backlog"
	"github.com/jbmopper/meristem/internal/convergence"
	"github.com/jbmopper/meristem/internal/cultivaractivation"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/errorreporting"
	"github.com/jbmopper/meristem/internal/feed"
	"github.com/jbmopper/meristem/internal/grants"
	"github.com/jbmopper/meristem/internal/httpconnector"
	"github.com/jbmopper/meristem/internal/oauth"
	"github.com/jbmopper/meristem/internal/policyprofile"
	"github.com/jbmopper/meristem/internal/projectiondefs"
	"github.com/jbmopper/meristem/internal/registry"
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
		s.toolProjectionsList(),
		s.toolProjectionsGet(),
		s.toolProjectionsDefine(),
		s.toolRegistryList(),
		s.toolRegistryGet(),
		s.toolRegistryDefineTropism(),
		s.toolRegistryDefineCultivar(),
		s.toolRegistryActivateCultivar(),
		s.toolDeterministicErrorsList(),
		s.toolDeterministicErrorsGet(),
		s.toolWorkItemsList(),
		s.toolWorkItemsGet(),
		s.toolApprovalsListForWorkItem(),
		s.toolApprovalsGet(),
		s.toolOAuthClientsBindActor(),
		s.toolOAuthClientsRevoke(),
		s.toolOAuthGrantsRevoke(),
		s.toolWorkItemsCreate(),
		s.toolWorkItemsSpawnChild(),
		s.toolWorkItemsAppendEvent(),
		s.toolApprovalsRequest(),
		s.toolApprovalsDecide(),
		s.toolHTTPConnectorRequest(),
		s.toolConvergenceProposeChecks(),
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

func (s *Server) toolOAuthClientsBindActor() Tool {
	return Tool{
		Name:        "oauth_clients.bind_actor",
		Description: "Bind a registered OAuth client to a pre-provisioned provider actor and sealed authority profile.",
		Mutates:     true,
		InputSchema: schemaObject([]string{"client_id", "actor_token_id", "authority_profile"}, map[string]any{
			"client_id":         schemaString("Registered OAuth client id."),
			"actor_token_id":    schemaString("Pre-provisioned provider actor token UUID."),
			"authority_profile": schemaString("Sealed provider authority profile."),
		}),
		Handler: func(ctx context.Context, actor domain.Token, raw json.RawMessage) (any, error) {
			if s.deps.OAuthClientAdmin == nil {
				return nil, errors.New("oauth client administration service not configured")
			}
			var args struct {
				ClientID         string    `json:"client_id"`
				ActorTokenID     uuid.UUID `json:"actor_token_id"`
				AuthorityProfile string    `json:"authority_profile"`
			}
			if err := decodeArgs(raw, &args); err != nil {
				return nil, err
			}
			if err := s.deps.OAuthClientAdmin.BindActor(ctx, args.ClientID, args.ActorTokenID, args.AuthorityProfile, actor); err != nil {
				return nil, oauthClientAdminToolErr(err)
			}
			return map[string]any{
				"client_id":         args.ClientID,
				"actor_token_id":    args.ActorTokenID,
				"authority_profile": args.AuthorityProfile,
			}, nil
		},
	}
}

func (s *Server) toolOAuthClientsRevoke() Tool {
	return Tool{
		Name:        "oauth_clients.revoke",
		Description: "Revoke a registered OAuth client and its current grants.",
		Mutates:     true,
		InputSchema: schemaObject([]string{"client_id"}, map[string]any{
			"client_id": schemaString("Registered OAuth client id."),
			"reason":    schemaString("Optional human-readable revocation reason."),
		}),
		Handler: func(ctx context.Context, actor domain.Token, raw json.RawMessage) (any, error) {
			if s.deps.OAuthClientAdmin == nil {
				return nil, errors.New("oauth client administration service not configured")
			}
			var args struct {
				ClientID string `json:"client_id"`
				Reason   string `json:"reason"`
			}
			if err := decodeArgs(raw, &args); err != nil {
				return nil, err
			}
			if err := s.deps.OAuthClientAdmin.Revoke(ctx, args.ClientID, args.Reason, actor); err != nil {
				return nil, oauthClientAdminToolErr(err)
			}
			// The canonical REST operation returns 204 with no response body.
			return nil, nil
		},
	}
}

func (s *Server) toolOAuthGrantsRevoke() Tool {
	return Tool{
		Name:        "oauth_grants.revoke",
		Description: "Revoke a single issued OAuth grant, invalidating its access and refresh tokens.",
		Mutates:     true,
		InputSchema: schemaObject([]string{"grant_id", "reason"}, map[string]any{
			"grant_id": schemaString("Issued OAuth grant UUID."),
			"reason":   schemaString("Human-readable revocation reason."),
		}),
		Handler: func(ctx context.Context, actor domain.Token, raw json.RawMessage) (any, error) {
			if s.deps.OAuthClientAdmin == nil {
				return nil, errors.New("oauth client administration service not configured")
			}
			var args struct {
				GrantID uuid.UUID `json:"grant_id"`
				Reason  string    `json:"reason"`
			}
			if err := decodeArgs(raw, &args); err != nil {
				return nil, err
			}
			if err := s.deps.OAuthClientAdmin.RevokeGrant(ctx, args.GrantID, args.Reason, actor); err != nil {
				return nil, oauthClientAdminToolErr(err)
			}
			// The canonical REST operation returns 204 with no response body.
			return nil, nil
		},
	}
}

func (s *Server) toolBacklogReadiness() Tool {
	return Tool{
		Name:        "backlog.readiness",
		Description: "Summarize the complete visible backlog from the work_items projection.",
		InputSchema: schemaObject(nil, map[string]any{
			"limit": schemaInt("Deprecated compatibility argument (0-200); readiness always scans the complete visible backlog."),
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
			items, err := s.deps.WorkItems.ListAll(ctx, "")
			if err != nil {
				return nil, err
			}
			items, err = s.filterWorkItems(ctx, actor, items)
			if err != nil {
				return nil, err
			}
			summary := backlog.Summarize(items, backlog.Options{
				Limit: 0,
				AsOf:  time.Now().UTC(),
			})
			if isProviderSafeContext(ctx) {
				// Hand the unreduced summary to the boundary renderer, which owns
				// the state_reason omission for the provider-safe wire shape.
				return providerSafeReadinessResult{summary: summary}, nil
			}
			return summary, nil
		},
	}
}

func (s *Server) toolProjectionsList() Tool {
	return Tool{
		Name:        "projections.list",
		Description: "List current named feed projections.",
		InputSchema: schemaObject(nil, nil),
		Handler: func(ctx context.Context, actor domain.Token, raw json.RawMessage) (any, error) {
			if s.deps.Projections == nil {
				return nil, errors.New("projection service not configured")
			}
			var args struct{}
			if err := decodeArgs(raw, &args); err != nil {
				return nil, err
			}
			return s.deps.Projections.List(ctx)
		},
	}
}

func (s *Server) toolProjectionsGet() Tool {
	return Tool{
		Name:        "projections.get",
		Description: "Fetch one named feed projection.",
		InputSchema: schemaObject([]string{"name"}, map[string]any{
			"name": schemaString("Projection name."),
		}),
		Handler: func(ctx context.Context, actor domain.Token, raw json.RawMessage) (any, error) {
			if s.deps.Projections == nil {
				return nil, errors.New("projection service not configured")
			}
			var args struct {
				Name string `json:"name"`
			}
			if err := decodeArgs(raw, &args); err != nil {
				return nil, err
			}
			item, err := s.deps.Projections.Get(ctx, args.Name)
			if err != nil {
				return nil, projectionToolErr(err)
			}
			return map[string]any{"projection": item}, nil
		},
	}
}

func (s *Server) toolProjectionsDefine() Tool {
	return Tool{
		Name:        "projections.define",
		Description: "Define the next version of a named feed projection.",
		Mutates:     true,
		InputSchema: schemaObject([]string{"name", "version", "filter"}, map[string]any{
			"name":        schemaString("Projection name ([a-z0-9][a-z0-9-]*)."),
			"version":     schemaInt("Version. Starts at 1; existing names require current+1."),
			"type":        schemaString("Projection type. Only feed is accepted; defaults to feed."),
			"rootstock":   schemaBool("Whether this definition is immutable rootstock."),
			"filter":      schemaAny("Feed filter object: {kinds, kind_classes}. Admin kinds/classes are refused."),
			"description": schemaString("Human-readable description."),
		}),
		Handler: func(ctx context.Context, actor domain.Token, raw json.RawMessage) (any, error) {
			if s.deps.Projections == nil {
				return nil, errors.New("projection service not configured")
			}
			var args projectiondefs.DefineInput
			if err := decodeArgs(raw, &args); err != nil {
				return nil, err
			}
			item, fresh, err := s.deps.Projections.Define(ctx, actor, args)
			if err != nil {
				return nil, projectionToolErr(err)
			}
			return map[string]any{"projection": item, "defined": fresh}, nil
		},
	}
}

func (s *Server) toolRegistryList() Tool {
	return Tool{
		Name:        "registry.list",
		Description: "List current tropism and cultivar registry entries.",
		InputSchema: schemaObject(nil, nil),
		Handler: func(ctx context.Context, actor domain.Token, raw json.RawMessage) (any, error) {
			if s.deps.Registry == nil {
				return nil, errors.New("registry service not configured")
			}
			var args struct{}
			if err := decodeArgs(raw, &args); err != nil {
				return nil, err
			}
			return s.deps.Registry.List(ctx)
		},
	}
}

func (s *Server) toolRegistryGet() Tool {
	return Tool{
		Name:        "registry.get",
		Description: "Fetch one registry entry by kind (tropism or cultivar) and name.",
		InputSchema: schemaObject([]string{"kind", "name"}, map[string]any{
			"kind": schemaString("Entry kind: tropism or cultivar."),
			"name": schemaString("Registry name."),
		}),
		Handler: func(ctx context.Context, actor domain.Token, raw json.RawMessage) (any, error) {
			if s.deps.Registry == nil {
				return nil, errors.New("registry service not configured")
			}
			var args struct {
				Kind string `json:"kind"`
				Name string `json:"name"`
			}
			if err := decodeArgs(raw, &args); err != nil {
				return nil, err
			}
			switch args.Kind {
			case "tropism":
				item, err := s.deps.Registry.GetTropism(ctx, args.Name)
				if err != nil {
					return nil, registryToolErr(err)
				}
				return map[string]any{"tropism": item}, nil
			case "cultivar":
				item, err := s.deps.Registry.GetCultivar(ctx, args.Name)
				if err != nil {
					return nil, registryToolErr(err)
				}
				return map[string]any{"cultivar": item}, nil
			default:
				return nil, replayableToolErr(fmt.Errorf("invalid_kind: kind must be tropism or cultivar"))
			}
		},
	}
}

func (s *Server) toolRegistryDefineTropism() Tool {
	return Tool{
		Name:        "registry.define_tropism",
		Description: "Define the next version of a tropism registry entry.",
		Mutates:     true,
		InputSchema: schemaObject([]string{"name", "version", "reducer"}, map[string]any{
			"name":        schemaString("Tropism name ([a-z0-9][a-z0-9-]*)."),
			"version":     schemaInt("Version. Starts at 1; existing names require current+1."),
			"reducer":     schemaAny("Reducer reference object: {identity, version}."),
			"params":      schemaAny("Reducer-specific JSON object. Defaults to {}."),
			"description": schemaString("Human-readable description."),
		}),
		Handler: func(ctx context.Context, actor domain.Token, raw json.RawMessage) (any, error) {
			if s.deps.Registry == nil {
				return nil, errors.New("registry service not configured")
			}
			var args registry.DefineTropismInput
			if err := decodeArgs(raw, &args); err != nil {
				return nil, err
			}
			item, fresh, err := s.deps.Registry.DefineTropism(ctx, actor, args)
			if err != nil {
				return nil, registryToolErr(err)
			}
			return map[string]any{"tropism": item, "defined": fresh}, nil
		},
	}
}

func (s *Server) toolRegistryDefineCultivar() Tool {
	return Tool{
		Name:        "registry.define_cultivar",
		Description: "Define the next version of a cultivar registry entry.",
		Mutates:     true,
		InputSchema: schemaObject([]string{"name", "version", "tropism", "profile", "xylem", "phloem"}, map[string]any{
			"name":        schemaString("Cultivar name ([a-z0-9][a-z0-9-]*)."),
			"version":     schemaInt("Version. Starts at 1; existing non-rootstock names require current+1."),
			"rootstock":   schemaBool("Whether this cultivar is immutable rootstock."),
			"tropism":     schemaAny("Tropism reference object: {name, version}."),
			"profile":     schemaAny("Worker profile object: {briefing, scopes_template}."),
			"xylem":       schemaAny("Budget envelope object: {max_attempts, max_wall_seconds, max_depth, max_children_per_item, max_concurrent_running_items_per_token, max_events_per_item_per_hour_by_class}."),
			"phloem":      schemaString("Projection/reference used for context flow."),
			"description": schemaString("Human-readable description."),
		}),
		Handler: func(ctx context.Context, actor domain.Token, raw json.RawMessage) (any, error) {
			if s.deps.Registry == nil {
				return nil, errors.New("registry service not configured")
			}
			var args registry.DefineCultivarInput
			if err := decodeArgs(raw, &args); err != nil {
				return nil, err
			}
			item, fresh, err := s.deps.Registry.DefineCultivar(ctx, actor, args)
			if err != nil {
				return nil, registryToolErr(err)
			}
			return map[string]any{"cultivar": item, "defined": fresh}, nil
		},
	}
}

func (s *Server) toolRegistryActivateCultivar() Tool {
	return Tool{
		Name:        "registry.activate_cultivar",
		Description: "Activate a worker-proposed non-rootstock cultivar through the work-item grant/review gate.",
		Mutates:     true,
		InputSchema: schemaObject([]string{"work_item_id", "name", "version", "tropism", "profile", "xylem", "phloem"}, map[string]any{
			"work_item_id": schemaString("Proposal work item uuid whose tree scopes and human_review_status gate activation."),
			"name":         schemaString("Cultivar name ([a-z0-9][a-z0-9-]*)."),
			"version":      schemaInt("Version. Starts at 1; existing non-rootstock names require current+1."),
			"rootstock":    schemaBool("Must be false for worker self-extension; rootstock uses owner migration path."),
			"tropism":      schemaAny("Tropism reference object: {name, version}."),
			"profile":      schemaAny("Worker profile object: {briefing, scopes_template}. {root} is resolved to work_item_id for the grant reducer."),
			"xylem":        schemaAny("Budget envelope object: {max_attempts, max_wall_seconds, max_depth, max_children_per_item, max_concurrent_running_items_per_token, max_events_per_item_per_hour_by_class}."),
			"phloem":       schemaString("Projection/reference used for context flow."),
			"description":  schemaString("Human-readable description."),
		}),
		Handler: func(ctx context.Context, actor domain.Token, raw json.RawMessage) (any, error) {
			if s.deps.CultivarActivations == nil {
				return nil, errors.New("cultivar activation service not configured")
			}
			var args struct {
				WorkItemID  uuid.UUID `json:"work_item_id"`
				Name        string    `json:"name"`
				Version     int       `json:"version"`
				Rootstock   bool      `json:"rootstock"`
				Tropism     any       `json:"tropism"`
				Profile     any       `json:"profile"`
				Xylem       any       `json:"xylem"`
				Phloem      string    `json:"phloem"`
				Description string    `json:"description"`
			}
			if err := decodeArgs(raw, &args); err != nil {
				return nil, err
			}
			var cultivar registry.DefineCultivarInput
			if err := decodeArgs(raw, &cultivar); err != nil {
				return nil, err
			}
			if err := s.canWriteWorkItem(ctx, actor, args.WorkItemID); err != nil {
				return nil, err
			}
			result, err := s.deps.CultivarActivations.Activate(ctx, cultivaractivation.ActivateInput{
				Actor:      actor,
				WorkItemID: args.WorkItemID,
				Cultivar:   cultivar,
			})
			if err != nil {
				return nil, cultivarActivationToolErr(err)
			}
			out := map[string]any{
				"activation_id": result.ActivationID,
				"work_item_id":  result.WorkItemID,
				"disposition":   result.Disposition,
				"reason":        result.Reason,
				"scopes":        result.Scopes,
				"events": map[string]any{
					"requested": result.RequestEventID,
					"outcome":   result.OutcomeEventID,
				},
			}
			if result.Cultivar != nil {
				out["cultivar"] = result.Cultivar
			}
			if result.EscalationID != uuid.Nil || result.HumanWorkItemID != uuid.Nil {
				out["escalation"] = map[string]any{
					"id":                 result.EscalationID,
					"human_work_item_id": result.HumanWorkItemID,
				}
			}
			return out, nil
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
				"captured_at":  res.CapturedAt,
			}, nil
		},
	}
}

func (s *Server) toolFeedRead() Tool {
	return Tool{
		Name: "feed.read",
		Description: "Read feed-visible events. Provider HTTP returns the provider_safe_feed.v1 structural projection without raw event payloads. Default: snapshot (newest first). " +
			"Pass cursor and/or wait (Go duration, e.g. 30s) for watcher mode — same contract as GET /v1/feed (oldest-first page, next_cursor, has_more). " +
			"scope=assigned and exclude_actor translate into the same normalized filter contract REST uses; filtered cursors are identity-bound and fail closed on mismatch.",
		InputSchema: schemaObject(nil, map[string]any{
			"limit":         schemaInt("Max items (1-200). Defaults to 50."),
			"projection":    schemaString("Optional named feed projection."),
			"cursor":        schemaString("Opaque cursor from a prior next_cursor or SSE id. Omit for snapshot mode."),
			"wait":          schemaString("Long-poll cap as a Go duration (e.g. 10s). Use with watcher semantics; server-capped."),
			"scope":         schemaString("Optional. \"assigned\" selects the reducing assigned/addressed lane — same contract as REST scope=assigned. Assigned-only tokens are normalized onto this lane automatically."),
			"exclude_actor": schemaStringArray("Optional actor exclusions: \"self\" or a token id, repeatable — same contract as REST exclude_actor. Malformed values fail the call closed."),
		}),
		Handler: func(ctx context.Context, actor domain.Token, raw json.RawMessage) (any, error) {
			if s.deps.Feed == nil {
				return nil, errors.New("feed service not configured")
			}
			var args struct {
				Limit        int      `json:"limit"`
				Projection   string   `json:"projection"`
				Cursor       string   `json:"cursor"`
				Wait         string   `json:"wait"`
				Scope        string   `json:"scope"`
				ExcludeActor []string `json:"exclude_actor"`
			}
			if err := decodeArgs(raw, &args); err != nil {
				return nil, err
			}
			var projection *projectiondefs.Projection
			if args.Projection != "" {
				if s.deps.Projections == nil {
					return nil, errors.New("projection service not configured")
				}
				item, err := s.deps.Projections.Get(ctx, args.Projection)
				if err != nil {
					return nil, projectionToolErr(err)
				}
				projection = &item
			}
			var assigned bool
			switch args.Scope {
			case "":
				assigned = access.RequiresAssignedFeed(actor)
			case "assigned":
				if !access.CanReadAssignedFeed(actor) {
					return nil, replayableToolErr(fmt.Errorf("insufficient_scope: token cannot read assigned feed"))
				}
				assigned = true
			default:
				return nil, fmt.Errorf("feed.read: invalid_feed_scope: scope must be assigned when present")
			}
			excluded := make([]uuid.UUID, 0, len(args.ExcludeActor))
			for _, value := range args.ExcludeActor {
				if value == "self" {
					excluded = append(excluded, actor.ID)
					continue
				}
				id, err := uuid.Parse(strings.TrimSpace(value))
				if err != nil || id == uuid.Nil {
					return nil, fmt.Errorf("feed.read: invalid_exclude_actor: exclude_actor must be self or a token id")
				}
				excluded = append(excluded, id)
			}
			// One contract: the identical normalized ReadFilter REST builds,
			// with the access reduction evaluated inside each scan batch so
			// unauthorized or filtered traffic can neither satisfy a wait nor
			// consume a limit. No post-hoc MCP-only filtering remains.
			readFilter := feed.ReadFilter{Projection: projectionFilterForTool(projection)}
			if assigned {
				readFilter.Predicates = append(readFilter.Predicates, feed.Predicate{
					Kind:    feed.PredicateAssignedOrAddressed,
					TokenID: actor.ID,
				})
			}
			for _, id := range excluded {
				readFilter.Predicates = append(readFilter.Predicates, feed.Predicate{
					Kind:    feed.PredicateExcludeActor,
					TokenID: id,
				})
			}
			readFilter.Reduce = s.feedAccessReduce(actor)
			readFilter, err := feed.NormalizeReadFilter(readFilter)
			if err != nil {
				return nil, fmt.Errorf("feed.read: invalid_filter: %w", err)
			}
			if args.Cursor == "" && args.Wait == "" {
				var items []feed.Item
				if !assigned && projection == nil && len(excluded) == 0 {
					// Preserve the legacy snapshot's byte-for-byte ordering for
					// plain broad readers — the same compatibility branch REST
					// keeps. The access reduction still applies, so scoped
					// visibility and feed-scope denial are unchanged; only the
					// contract-filtered reads go through ListWithReadFilter.
					items, err = s.deps.Feed.List(ctx, args.Limit)
					if err == nil {
						items, err = s.feedAccessReduce(actor)(ctx, items)
					}
				} else {
					items, err = s.deps.Feed.ListWithReadFilter(ctx, readFilter, args.Limit)
				}
				if err != nil {
					if errors.Is(err, access.ErrDenied) {
						return nil, replayableToolErr(fmt.Errorf("insufficient_scope: token cannot read feed"))
					}
					return nil, err
				}
				if isProviderSafeContext(ctx) {
					return providerSafeFeedSnapshot{items: items}, nil
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
				Cursor:            args.Cursor,
				Wait:              wait,
				Limit:             args.Limit,
				ProjectionName:    projectionNameForTool(projection),
				ProjectionVersion: projectionVersionForTool(projection),
				ReadFilter:        &readFilter,
			})
			if err != nil {
				if errors.Is(err, feed.ErrInvalidCursor) {
					return nil, fmt.Errorf("feed.read: invalid_cursor: %w", err)
				}
				if errors.Is(err, feed.ErrCursorProjectionMismatch) {
					return nil, fmt.Errorf("feed.read: cursor_projection_mismatch: %w", err)
				}
				if errors.Is(err, feed.ErrCursorFilterMismatch) {
					return nil, fmt.Errorf("feed.read: cursor_filter_mismatch: %w", err)
				}
				if errors.Is(err, access.ErrDenied) {
					return nil, replayableToolErr(fmt.Errorf("insufficient_scope: token cannot read feed"))
				}
				return nil, err
			}
			if isProviderSafeContext(ctx) {
				return providerSafeFeedPage{page: page}, nil
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
			if isProviderSafeContext(ctx) {
				return providerSafeWorkItemsResult{items: items}, nil
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
			if isProviderSafeContext(ctx) {
				return providerSafeWorkItemResult{item: item}, nil
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
			"cultivar": schemaString(
				"Optional launch cultivar reference, normalized to name@version.",
			),
			"patience_budget_seconds": schemaInt(
				"Optional explicit patience budget in seconds. Zero means use cultivar or policy fallback.",
			),
			"escalation_rule": schemaString(
				"Optional timeout escalation rule. Currently hand_to_human.",
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
				Cultivar                   string   `json:"cultivar"`
				PatienceBudgetSeconds      int      `json:"patience_budget_seconds"`
				EscalationRule             string   `json:"escalation_rule"`
			}
			if err := decodeArgs(raw, &args); err != nil {
				return nil, err
			}
			if err := validateWorkItemCreateArgs(args.Title, args.State, args.SuggestedConvergenceChecks, args.HumanReviewStatus); err != nil {
				return nil, err
			}
			if err := validateWorkItemLaunchArgs(args.PatienceBudgetSeconds, args.EscalationRule); err != nil {
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
				Cultivar:                   args.Cultivar,
				PatienceBudgetSeconds:      args.PatienceBudgetSeconds,
				EscalationRule:             domain.EscalationRule(args.EscalationRule),
				Actor:                      actor,
			})
			if err != nil {
				return nil, workItemToolErr(err, nil)
			}
			if isProviderSafeContext(ctx) {
				return providerSafeWorkItemResult{item: item, echoID: true}, nil
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
			"cultivar": schemaString(
				"Optional launch cultivar reference, normalized to name@version.",
			),
			"patience_budget_seconds": schemaInt(
				"Optional explicit patience budget in seconds. Zero means use cultivar or policy fallback.",
			),
			"escalation_rule": schemaString(
				"Optional timeout escalation rule. Currently hand_to_human.",
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
				Cultivar                   string   `json:"cultivar"`
				PatienceBudgetSeconds      int      `json:"patience_budget_seconds"`
				EscalationRule             string   `json:"escalation_rule"`
			}
			if err := decodeArgs(raw, &args); err != nil {
				return nil, err
			}
			if err := validateWorkItemCreateArgs(args.Title, args.State, args.SuggestedConvergenceChecks, args.HumanReviewStatus); err != nil {
				return nil, err
			}
			if err := validateWorkItemLaunchArgs(args.PatienceBudgetSeconds, args.EscalationRule); err != nil {
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
				Cultivar:                   args.Cultivar,
				PatienceBudgetSeconds:      args.PatienceBudgetSeconds,
				EscalationRule:             domain.EscalationRule(args.EscalationRule),
				Actor:                      actor,
			})
			if err != nil {
				return nil, workItemToolErr(err, fmt.Errorf("parent work item %s not found", parent))
			}
			if isProviderSafeContext(ctx) {
				return providerSafeWorkItemResult{item: item, echoID: true, parentID: &parent}, nil
			}
			return map[string]any{
				"parent_id":    parent,
				"work_item_id": item.ID,
				"work_item":    toWorkItemDTO(item),
			}, nil
		},
	}
}

func (s *Server) toolHTTPConnectorRequest() Tool {
	return Tool{
		Name:        "connectors.http_request",
		Description: "Request a generic HTTP connector action. Write-mode actions create an approval and do not perform outbound writes.",
		Mutates:     true,
		InputSchema: schemaObject([]string{"work_item_id", "mode", "url"}, map[string]any{
			"work_item_id": schemaString("Work item uuid."),
			"mode":         schemaString("Action mode: read or write."),
			"method":       schemaString("HTTP method. Read defaults to GET; write defaults to POST."),
			"url":          schemaString("Absolute http or https URL."),
			"body":         schemaAny("Optional JSON request body. No credentials or secrets."),
		}),
		Handler: func(ctx context.Context, actor domain.Token, raw json.RawMessage) (any, error) {
			if s.deps.HTTPConnector == nil {
				return nil, errors.New("http connector service not configured")
			}
			var args struct {
				WorkItemID string          `json:"work_item_id"`
				Mode       string          `json:"mode"`
				Method     string          `json:"method"`
				URL        string          `json:"url"`
				Body       json.RawMessage `json:"body"`
			}
			if err := decodeArgs(raw, &args); err != nil {
				return nil, err
			}
			workItemID, err := parseUUID(args.WorkItemID, "work_item_id")
			if err != nil {
				return nil, err
			}
			if err := s.canWriteWorkItem(ctx, actor, workItemID); err != nil {
				return nil, err
			}
			result, err := s.deps.HTTPConnector.Request(ctx, httpconnector.RequestInput{
				WorkItemID: workItemID,
				Mode:       httpconnector.Mode(args.Mode),
				Method:     args.Method,
				URL:        args.URL,
				Body:       args.Body,
				Actor:      actor,
			})
			if err != nil {
				return nil, httpConnectorToolErr(err)
			}
			out := map[string]any{
				"action":   result.Action,
				"created":  result.Fresh,
				"event_id": result.EventID,
			}
			if result.Approval != nil {
				out["approval"] = result.Approval
				out["approval_event_id"] = result.ApprovalEvent
			}
			return out, nil
		},
	}
}

func (s *Server) toolApprovalsListForWorkItem() Tool {
	return Tool{
		Name:        "approvals.list_for_work_item",
		Description: "List approvals associated with a visible work item.",
		InputSchema: schemaObject([]string{"work_item_id"}, map[string]any{
			"work_item_id": schemaString("Work item uuid."),
		}),
		Handler: func(ctx context.Context, actor domain.Token, raw json.RawMessage) (any, error) {
			if s.deps.Approvals == nil {
				return nil, errors.New("approval service not configured")
			}
			var args struct {
				WorkItemID string `json:"work_item_id"`
			}
			if err := decodeArgs(raw, &args); err != nil {
				return nil, err
			}
			workItemID, err := parseUUID(args.WorkItemID, "work_item_id")
			if err != nil {
				return nil, err
			}
			if err := s.canReadWorkItem(ctx, actor, workItemID); err != nil {
				return nil, err
			}
			items, err := s.deps.Approvals.ListForWorkItem(ctx, workItemID)
			if err != nil {
				return nil, err
			}
			return map[string]any{"items": items}, nil
		},
	}
}

func (s *Server) toolApprovalsGet() Tool {
	return Tool{
		Name:        "approvals.get",
		Description: "Fetch one approval if its work item is visible to this token.",
		InputSchema: schemaObject([]string{"id"}, map[string]any{
			"id": schemaString("Approval uuid."),
		}),
		Handler: func(ctx context.Context, actor domain.Token, raw json.RawMessage) (any, error) {
			if s.deps.Approvals == nil {
				return nil, errors.New("approval service not configured")
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
			item, err := s.deps.Approvals.Get(ctx, id)
			if err != nil {
				return nil, approvalToolErr(err)
			}
			if err := s.canReadWorkItem(ctx, actor, item.WorkItemID); err != nil {
				return nil, err
			}
			return map[string]any{"approval": item}, nil
		},
	}
}

func (s *Server) toolApprovalsRequest() Tool {
	return Tool{
		Name:        "approvals.request",
		Description: "Request approval for a side effect and park the work item in awaiting_approval.",
		Mutates:     true,
		InputSchema: schemaObject([]string{"work_item_id", "summary"}, map[string]any{
			"work_item_id":       schemaString("Work item uuid."),
			"summary":            schemaString("Short approval summary."),
			"request":            schemaAny("Structured approval request payload. Defaults to {}."),
			"expires_in_seconds": schemaInt("Optional expiry duration in seconds. Defaults to 3600."),
		}),
		Handler: func(ctx context.Context, actor domain.Token, raw json.RawMessage) (any, error) {
			if s.deps.Approvals == nil {
				return nil, errors.New("approval service not configured")
			}
			var args struct {
				WorkItemID       string          `json:"work_item_id"`
				Summary          string          `json:"summary"`
				Request          json.RawMessage `json:"request"`
				ExpiresInSeconds int             `json:"expires_in_seconds"`
			}
			if err := decodeArgs(raw, &args); err != nil {
				return nil, err
			}
			workItemID, err := parseUUID(args.WorkItemID, "work_item_id")
			if err != nil {
				return nil, err
			}
			if err := s.canWriteWorkItem(ctx, actor, workItemID); err != nil {
				return nil, err
			}
			var request any
			if len(args.Request) > 0 {
				if err := json.Unmarshal(args.Request, &request); err != nil {
					return nil, replayableToolErr(fmt.Errorf("request: %w", err))
				}
			}
			result, err := s.deps.Approvals.Create(ctx, approvals.CreateInput{
				WorkItemID: workItemID,
				Summary:    args.Summary,
				Request:    request,
				ExpiresIn:  time.Duration(args.ExpiresInSeconds) * time.Second,
				Actor:      actor,
			})
			if err != nil {
				return nil, approvalToolErr(err)
			}
			return map[string]any{
				"approval": result.Approval,
				"created":  result.Fresh,
				"event_id": result.EventID,
			}, nil
		},
	}
}

func (s *Server) toolApprovalsDecide() Tool {
	return Tool{
		Name:        "approvals.decide",
		Description: "Approve or deny a pending approval. Requires a human non-root decision token.",
		Mutates:     true,
		InputSchema: schemaObject([]string{"id", "decision"}, map[string]any{
			"id":       schemaString("Approval uuid."),
			"decision": schemaString("Decision: approved or denied."),
			"reason":   schemaString("Optional decision reason."),
		}),
		Handler: func(ctx context.Context, actor domain.Token, raw json.RawMessage) (any, error) {
			if s.deps.Approvals == nil {
				return nil, errors.New("approval service not configured")
			}
			var args struct {
				ID       string `json:"id"`
				Decision string `json:"decision"`
				Reason   string `json:"reason"`
			}
			if err := decodeArgs(raw, &args); err != nil {
				return nil, err
			}
			id, err := parseUUID(args.ID, "id")
			if err != nil {
				return nil, err
			}
			if !access.ToolVisible(actor, "approvals.decide") {
				return nil, replayableToolErr(fmt.Errorf("insufficient_scope: token cannot decide approvals"))
			}
			result, err := s.deps.Approvals.Decide(ctx, approvals.DecisionInput{
				ApprovalID: id,
				Decision:   approvals.Decision(args.Decision),
				Reason:     args.Reason,
				Actor:      actor,
			})
			if err != nil {
				return nil, approvalToolErr(err)
			}
			return map[string]any{
				"approval": result.Approval,
				"decided":  result.Fresh,
				"event_id": result.EventID,
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
			"payload": schemaObjectAny("JSON OBJECT payload describing the event. Must be the object itself - a JSON-encoded string of the object is rejected as double-encoded."),
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
				return nil, workItemToolErr(err, fmt.Errorf("work item %s not found", id))
			}
			if isProviderSafeContext(ctx) {
				return providerSafeAppendEventResult{workItemID: id, appended: true}, nil
			}
			return map[string]any{"work_item_id": id, "appended": true}, nil
		},
	}
}

func (s *Server) toolConvergenceProposeChecks() Tool {
	return Tool{
		Name:        "convergence.propose_checks",
		Description: "Propose suggested convergence checks for a parent work item from its convergence-scribe child.",
		Mutates:     true,
		InputSchema: schemaObject([]string{"id", "proposal_of", "checks", "classified"}, map[string]any{
			"id":          schemaString("Parent work item uuid."),
			"proposal_of": schemaString("Convergence-scribe child work item uuid."),
			"checks": schemaStringArray(
				"Proposed checks. Machine checks use cmd:, event:, or query:. Human checks use human-ack:.",
			),
			"classified": schemaAny("Array of {check, class}; class is machine or human."),
			"rationale":  schemaString("Short rationale for the proposed checks."),
			"cultivar":   schemaString("Launch metadata from the scribe child, e.g. convergence-scribe@1."),
		}),
		Handler: func(ctx context.Context, actor domain.Token, raw json.RawMessage) (any, error) {
			if s.deps.CheckProposals == nil {
				return nil, errors.New("convergence proposal service not configured")
			}
			var args struct {
				ID         string                            `json:"id"`
				ProposalOf string                            `json:"proposal_of"`
				Checks     []string                          `json:"checks"`
				Classified []convergence.CheckClassification `json:"classified"`
				Rationale  string                            `json:"rationale"`
				Cultivar   string                            `json:"cultivar"`
			}
			if err := decodeArgs(raw, &args); err != nil {
				return nil, err
			}
			id, err := parseUUID(args.ID, "id")
			if err != nil {
				return nil, err
			}
			proposalOf, err := parseUUID(args.ProposalOf, "proposal_of")
			if err != nil {
				return nil, err
			}
			if err := s.canWriteWorkItem(ctx, actor, id); err != nil {
				return nil, err
			}
			result, err := s.deps.CheckProposals.ProposeChecks(ctx, id, convergence.ProposeChecksInput{
				ProposalOf: proposalOf,
				Checks:     args.Checks,
				Classified: args.Classified,
				Rationale:  args.Rationale,
				Cultivar:   args.Cultivar,
			}, actor)
			if err != nil {
				if errors.Is(err, convergence.ErrChecksProposalNotFound) {
					return nil, replayableToolErr(fmt.Errorf("work item %s not found", id))
				}
				return nil, err
			}
			return result, nil
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
				return nil, workItemToolErr(err, fmt.Errorf("work item %s not found", id))
			}
			if isProviderSafeContext(ctx) {
				return providerSafeWorkItemResult{item: item}, nil
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
				return nil, workItemToolErr(err, fmt.Errorf("work item %s not found", id))
			}
			if isProviderSafeContext(ctx) {
				return providerSafeWorkItemResult{item: item}, nil
			}
			return map[string]any{"work_item": toWorkItemDTO(item)}, nil
		},
	}
}

// feedAccessReduce is MCP's instance of the shared-contract authorization
// reducer: the same access reduction REST installs as ReadFilter.Reduce,
// evaluated inside each scan batch. The caller maps access.ErrDenied to a
// scoped tool error after the read returns.
func (s *Server) feedAccessReduce(actor domain.Token) feed.BatchReducer {
	return func(ctx context.Context, items []feed.Item) ([]feed.Item, error) {
		if s.deps.Access == nil {
			if access.RequiresScopedPolicy(actor) {
				return nil, errors.New("access service not configured")
			}
			return items, nil
		}
		return s.deps.Access.FilterFeedItems(ctx, actor, items)
	}
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

func workItemToolErr(err error, notFound error) error {
	if err == nil {
		return nil
	}
	if mapped := registryToolErr(err); isReplayableToolError(mapped) {
		return mapped
	}
	switch {
	case errors.Is(err, workitems.ErrNotFound):
		if notFound != nil {
			return replayableToolErr(notFound)
		}
		return replayableToolErr(fmt.Errorf("work_item_not_found: work item not found"))
	case errors.Is(err, workitems.ErrInvalidRequest),
		errors.Is(err, workitems.ErrInvalidState),
		errors.Is(err, workitems.ErrInvalidTransition),
		errors.Is(err, workitems.ErrRelationCycle),
		errors.Is(err, workitems.ErrConvergenceChecksRequired),
		errors.Is(err, workitems.ErrXylemBudgetExhausted),
		errors.Is(err, workitems.ErrUnexpectedEventDedupe):
		return replayableToolErr(err)
	default:
		return err
	}
}

func registryToolErr(err error) error {
	switch {
	case errors.Is(err, registry.ErrInvalidName),
		errors.Is(err, registry.ErrInvalidVersion),
		errors.Is(err, registry.ErrInvalidPayload),
		errors.Is(err, registry.ErrUnknownReducer),
		errors.Is(err, registry.ErrUnknownTropism),
		errors.Is(err, registry.ErrUnknownCultivar),
		errors.Is(err, registry.ErrVersionConflict),
		errors.Is(err, registry.ErrRootstockImmutable):
		return replayableToolErr(err)
	default:
		return err
	}
}

func cultivarActivationToolErr(err error) error {
	switch {
	case errors.Is(err, grants.ErrWorkItemNotFound):
		return replayableToolErr(fmt.Errorf("work_item_not_found: work item not found"))
	case errors.Is(err, registry.ErrInvalidName),
		errors.Is(err, registry.ErrInvalidVersion),
		errors.Is(err, registry.ErrInvalidPayload),
		errors.Is(err, registry.ErrUnknownReducer),
		errors.Is(err, registry.ErrUnknownTropism),
		errors.Is(err, registry.ErrUnknownCultivar),
		errors.Is(err, registry.ErrVersionConflict),
		errors.Is(err, registry.ErrRootstockImmutable):
		return replayableToolErr(err)
	default:
		return err
	}
}

func projectionToolErr(err error) error {
	switch {
	case errors.Is(err, projectiondefs.ErrInvalidName),
		errors.Is(err, projectiondefs.ErrInvalidVersion),
		errors.Is(err, projectiondefs.ErrInvalidPayload),
		errors.Is(err, projectiondefs.ErrUnknownProjection),
		errors.Is(err, projectiondefs.ErrUnknownKind),
		errors.Is(err, projectiondefs.ErrUnknownKindClass),
		errors.Is(err, projectiondefs.ErrNotProjectable),
		errors.Is(err, projectiondefs.ErrVersionConflict),
		errors.Is(err, projectiondefs.ErrRootstockImmutable):
		return replayableToolErr(err)
	default:
		return err
	}
}

func approvalToolErr(err error) error {
	switch {
	case errors.Is(err, approvals.ErrNotFound):
		return replayableToolErr(fmt.Errorf("approval_not_found: approval not found"))
	case errors.Is(err, approvals.ErrHumanDecisionToken):
		return replayableToolErr(fmt.Errorf("human_decision_token_required: approval decision requires a human non-root token"))
	case errors.Is(err, approvals.ErrSeparationOfDuties):
		return replayableToolErr(fmt.Errorf("separation_of_duties: requesting token cannot decide the same approval"))
	case errors.Is(err, approvals.ErrAlreadyDecided):
		return replayableToolErr(err)
	case errors.Is(err, approvals.ErrInvalidDecision):
		return replayableToolErr(fmt.Errorf("invalid_decision: decision must be approved or denied"))
	case errors.Is(err, approvals.ErrInvalidRequest):
		return replayableToolErr(err)
	default:
		return err
	}
}

func oauthClientAdminToolErr(err error) error {
	switch {
	case errors.Is(err, oauth.ErrOAuthClientAdminDenied):
		return replayableToolErr(errors.New("oauth_client_admin_denied: explicit non-root human OAuth client administration scope required"))
	case errors.Is(err, oauth.ErrClientNotFound):
		return replayableToolErr(errors.New("oauth_client_not_found: OAuth client not found"))
	case errors.Is(err, oauth.ErrGrantNotFound):
		return replayableToolErr(errors.New("oauth_grant_not_found: OAuth grant not found"))
	case errors.Is(err, oauth.ErrInvalidClientAdminInput):
		return replayableToolErr(fmt.Errorf("invalid_oauth_client_admin_request: %w", err))
	case errors.Is(err, oauth.ErrOAuthClientConflict):
		return replayableToolErr(fmt.Errorf("oauth_client_conflict: %w", err))
	default:
		return err
	}
}

func httpConnectorToolErr(err error) error {
	switch {
	case errors.Is(err, httpconnector.ErrNotFound):
		return replayableToolErr(fmt.Errorf("work_item_not_found: work item not found"))
	case errors.Is(err, httpconnector.ErrInvalidMode):
		return replayableToolErr(fmt.Errorf("invalid_connector_mode: mode must be read or write"))
	case errors.Is(err, httpconnector.ErrInvalidMethod),
		errors.Is(err, httpconnector.ErrInvalidURL),
		errors.Is(err, httpconnector.ErrUnsupportedRequest),
		errors.Is(err, httpconnector.ErrApprovalRequired):
		return replayableToolErr(err)
	default:
		return err
	}
}

func projectionNameForTool(p *projectiondefs.Projection) string {
	if p == nil {
		return ""
	}
	return p.Name
}

func projectionVersionForTool(p *projectiondefs.Projection) int {
	if p == nil {
		return 0
	}
	return p.Version
}

func projectionFilterForTool(p *projectiondefs.Projection) *feed.ProjectionFilter {
	if p == nil {
		return nil
	}
	return &p.Filter
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

// ProviderSafeWorkItemsContract names the reduced provider-facing work-item
// shape. Body and convergence checks are the ordinary non-private tracker
// instructions and remain useful to an owner connector. Free-form state reason
// and creator token id are omitted; private/encrypted material belongs outside
// work_item.body and is never joined into this DTO.
const ProviderSafeWorkItemsContract = "provider_safe_work_items.v1"

type providerSafeWorkItemDTO struct {
	ID                         uuid.UUID                `json:"id"`
	Title                      string                   `json:"title"`
	Body                       string                   `json:"body"`
	State                      domain.WorkItemState     `json:"state"`
	SuggestedConvergenceChecks []string                 `json:"suggested_convergence_checks"`
	HumanReviewStatus          domain.HumanReviewStatus `json:"human_review_status"`
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

func toProviderSafeWorkItemDTO(item domain.WorkItem) providerSafeWorkItemDTO {
	return providerSafeWorkItemDTO{
		ID:                         item.ID,
		Title:                      item.Title,
		Body:                       item.Body,
		State:                      item.State,
		SuggestedConvergenceChecks: item.SuggestedConvergenceChecks,
		HumanReviewStatus:          item.HumanReviewStatus,
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

func validateWorkItemLaunchArgs(patienceBudgetSeconds int, escalationRule string) error {
	if patienceBudgetSeconds < 0 {
		return replayableToolErr(errors.New("workitems: patience_budget_seconds must be >= 0"))
	}
	if escalationRule != "" && !domain.EscalationRule(escalationRule).Valid() {
		return replayableToolErr(fmt.Errorf("workitems: invalid escalation_rule %q", escalationRule))
	}
	return nil
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
	if properties == nil {
		properties = map[string]any{}
	}
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

// schemaObjectAny types a parameter as a JSON object without constraining its
// properties. Typeless parameters get marshaled as strings by some MCP
// clients, which is exactly the double-encoding defect the append seam now
// rejects - the declared type keeps conformant clients shape-faithful.
func schemaObjectAny(description string) map[string]any {
	return map[string]any{"type": "object", "description": description}
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
