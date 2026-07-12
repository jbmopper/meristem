package mcp

import (
	"encoding/json"
	"fmt"
	"strings"
)

// HTTPToolProfile is a fail-closed provider-facing tool policy. The allowed
// set controls both tools/list and tools/call; validateCall can further narrow
// mutation arguments before any handler, event append, queue write, or outbox
// write is reachable.
type HTTPToolProfile struct {
	name         string
	allowedTools map[string]bool
	validateCall func(tool string, arguments json.RawMessage) error
}

// ProviderSafeReadHTTPProfile exposes only tracker-shaped reads. In
// particular, it deliberately excludes the generic event feed until the safe
// provider feed projection is wired, plus registry, approval, policy, inbox,
// connector, convergence, and deterministic-error surfaces.
func ProviderSafeReadHTTPProfile() *HTTPToolProfile {
	return &HTTPToolProfile{
		name: "provider-safe-read",
		allowedTools: toolSet(
			"backlog.readiness",
			"work_items.list",
			"work_items.get",
		),
	}
}

// ProviderTrackerHTTPProfile adds the narrow tracker mutation surface to the
// provider-safe reads. These mutations use the ordinary MCP idempotency
// executor. Argument validation keeps this transport from granting latent job
// execution authority: new items must remain human-review-blocked and cannot
// name a cultivar, metadata writes cannot wave items through, and lifecycle
// transitions are limited to blocked or terminal states.
//
// Broader tracker transitions require a durable domain-level non-dispatchable
// marker that every worker reconciler enforces. Until that exists, widening
// this profile is unsafe even while the job executor is paused.
func ProviderTrackerHTTPProfile() *HTTPToolProfile {
	read := ProviderSafeReadHTTPProfile()
	for name := range trackerMutationHTTPTools() {
		read.allowedTools[name] = true
	}
	read.name = "provider-tracker"
	read.validateCall = validateProviderTrackerCall
	return read
}

func trackerMutationHTTPTools() map[string]bool {
	return toolSet(
		"work_items.create",
		"work_items.spawn_child",
		"work_items.append_event",
		"work_items.update_metadata",
		"work_items.transition",
	)
}

func toolSet(names ...string) map[string]bool {
	out := make(map[string]bool, len(names))
	for _, name := range names {
		out[name] = true
	}
	return out
}

func (o HTTPOptions) allowedTools() map[string]bool {
	if o.Profile != nil {
		return o.Profile.allowedTools
	}
	return o.AllowedTools
}

func (p HTTPToolProfile) Name() string { return p.name }

func validateProviderTrackerCall(tool string, raw json.RawMessage) error {
	switch tool {
	case "work_items.create", "work_items.spawn_child":
		var args struct {
			HumanReviewStatus string `json:"human_review_status"`
			Cultivar          string `json:"cultivar"`
			State             string `json:"state"`
		}
		if err := decodeHTTPProfileArgs(raw, &args); err != nil {
			return err
		}
		if args.HumanReviewStatus != "blocked" {
			return executionAuthorityDenied(tool + " requires human_review_status=blocked")
		}
		if strings.TrimSpace(args.Cultivar) != "" {
			return executionAuthorityDenied(tool + " cannot assign a cultivar")
		}
		switch args.State {
		case "", "captured", "triaged", "planned", "blocked":
			return nil
		default:
			return executionAuthorityDenied(tool + " cannot create an execution-shaped lifecycle state")
		}

	case "work_items.update_metadata":
		var args struct {
			HumanReviewStatus string `json:"human_review_status"`
		}
		if err := decodeHTTPProfileArgs(raw, &args); err != nil {
			return err
		}
		if args.HumanReviewStatus != "blocked" {
			return executionAuthorityDenied(tool + " cannot wave through or approve work")
		}
		return nil

	case "work_items.transition":
		var args struct {
			To string `json:"to"`
		}
		if err := decodeHTTPProfileArgs(raw, &args); err != nil {
			return err
		}
		switch args.To {
		case "blocked", "done", "failed", "canceled":
			return nil
		default:
			return executionAuthorityDenied(tool + " may only block or terminalize work")
		}

	case "work_items.append_event":
		var args struct {
			Kind string `json:"kind"`
		}
		if err := decodeHTTPProfileArgs(raw, &args); err != nil {
			return err
		}
		switch args.Kind {
		case "provider.note", "provider.progress":
			return nil
		default:
			return executionAuthorityDenied(tool + " may only append provider.note or provider.progress")
		}
	default:
		return fmt.Errorf("tool not enabled on provider-tracker HTTP MCP profile: %s", tool)
	}
}

func decodeHTTPProfileArgs(raw json.RawMessage, dst any) error {
	if len(raw) == 0 {
		raw = json.RawMessage(`{}`)
	}
	if err := json.Unmarshal(raw, dst); err != nil {
		return fmt.Errorf("invalid tools/call arguments: %w", err)
	}
	return nil
}

func executionAuthorityDenied(detail string) error {
	return fmt.Errorf("tracker_execution_authority_denied: %s", detail)
}

type httpToolDescriptor struct {
	toolDescriptor
	Annotations httpToolAnnotations `json:"annotations"`
}

type httpToolAnnotations struct {
	ReadOnlyHint    bool `json:"readOnlyHint"`
	DestructiveHint bool `json:"destructiveHint"`
	IdempotentHint  bool `json:"idempotentHint"`
	OpenWorldHint   bool `json:"openWorldHint"`
}

func httpAnnotationsForTool(tool Tool) httpToolAnnotations {
	destructive := false
	if tool.Name == "work_items.update_metadata" || tool.Name == "work_items.transition" {
		destructive = true
	}
	return httpToolAnnotations{
		ReadOnlyHint:    !tool.Mutates,
		DestructiveHint: destructive,
		IdempotentHint:  tool.Mutates,
		OpenWorldHint:   false,
	}
}
