package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/backlog"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/feed"
	"github.com/jbmopper/meristem/internal/grants"
	"github.com/jbmopper/meristem/internal/idempotency"
	"github.com/jbmopper/meristem/internal/safety"
	"github.com/jbmopper/meristem/internal/workitems"
)

type workItemResponse struct {
	ID                         uuid.UUID                `json:"id"`
	Title                      string                   `json:"title"`
	Body                       string                   `json:"body"`
	State                      domain.WorkItemState     `json:"state"`
	StateReason                *string                  `json:"state_reason,omitempty"`
	SuggestedConvergenceChecks []string                 `json:"suggested_convergence_checks"`
	HumanReviewStatus          domain.HumanReviewStatus `json:"human_review_status"`
	CreatedBy                  *uuid.UUID               `json:"created_by,omitempty"`
	CreatedAt                  time.Time                `json:"created_at"`
	StateEnteredAt             time.Time                `json:"state_entered_at"`
	UpdatedAt                  time.Time                `json:"updated_at"`
}

type subactorGrantResponse struct {
	GrantID     uuid.UUID                `json:"grant_id"`
	WorkItemID  uuid.UUID                `json:"work_item_id"`
	Template    grants.Template          `json:"template"`
	Disposition grants.Disposition       `json:"disposition"`
	Reason      string                   `json:"reason"`
	Scopes      []string                 `json:"scopes,omitempty"`
	Token       *subactorGrantToken      `json:"token,omitempty"`
	TokenSecret string                   `json:"token_secret,omitempty"`
	Events      subactorGrantEvents      `json:"events"`
	Escalation  *subactorGrantEscalation `json:"escalation,omitempty"`
}

type subactorGrantToken struct {
	ID     uuid.UUID     `json:"id"`
	Name   string        `json:"name"`
	Source domain.Source `json:"source"`
	Scopes []string      `json:"scopes"`
}

type subactorGrantEvents struct {
	Requested uuid.UUID `json:"requested"`
	Outcome   uuid.UUID `json:"outcome"`
}

type subactorGrantEscalation struct {
	ID              uuid.UUID `json:"id"`
	HumanWorkItemID uuid.UUID `json:"human_work_item_id"`
}

func (s *Server) handleCaptureMessage(w http.ResponseWriter, r *http.Request) {
	actor, ok := authenticatedToken(w, r)
	if !ok {
		return
	}
	if actor.Source != "" && actor.Source != domain.SourceHuman {
		writeAPIError(w, http.StatusForbidden, "human_token_required", "inbox message capture requires a human token")
		return
	}
	var req struct {
		Source string `json:"source"`
		Text   string `json:"text"`
	}
	if !decodeJSONRequest(w, r, &req) {
		return
	}
	source := req.Source
	if source == "" {
		source = string(domain.SourceHuman)
	}
	if source != string(domain.SourceHuman) {
		writeAPIError(w, http.StatusBadRequest, "invalid_source", "v0 inbox messages only accept source=human")
		return
	}
	result, err := s.inbox.CaptureText(r.Context(), actor, req.Text)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "message_capture_failed", err.Error())
		return
	}
	capturedAt := time.Now().UTC()
	if item, err := s.workItems.Get(r.Context(), result.WorkItemID); err == nil {
		capturedAt = item.CreatedAt
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"message_id":   result.MessageID,
		"work_item_id": result.WorkItemID,
		"captured_at":  capturedAt,
	})
}

func (s *Server) handleCreateSubactorGrant(w http.ResponseWriter, r *http.Request) {
	actor, ok := authenticatedToken(w, r)
	if !ok {
		return
	}
	if s.grants == nil {
		writeAPIError(w, http.StatusServiceUnavailable, "service_unavailable", "subactor grant service is not configured")
		return
	}
	var req struct {
		Template        string    `json:"template"`
		WorkItemID      uuid.UUID `json:"work_item_id"`
		RequestedScopes []string  `json:"requested_scopes"`
		Name            string    `json:"name"`
	}
	if !decodeJSONRequest(w, r, &req) {
		return
	}
	result, err := s.grants.Issue(r.Context(), grants.IssueInput{
		Parent:          actor,
		WorkItemID:      req.WorkItemID,
		Template:        grants.Template(req.Template),
		RequestedScopes: req.RequestedScopes,
		Name:            req.Name,
	})
	if err != nil {
		writeGrantError(w, err)
		return
	}
	resp := toSubactorGrantResponse(result)
	if resp.TokenSecret != "" {
		redacted := resp
		redacted.TokenSecret = ""
		recorded, err := json.Marshal(redacted)
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, "subactor_grant_failed", "could not encode redacted idempotency response")
			return
		}
		idempotency.SetRecordedResponse(r.Context(), recorded)
	}
	writeJSON(w, http.StatusCreated, resp)
}

func (s *Server) handlePanicRevokeTokens(w http.ResponseWriter, r *http.Request) {
	actor, ok := authenticatedToken(w, r)
	if !ok {
		return
	}
	revoked, err := s.authService.RevokeAllNonRoot(r.Context(), actor)
	if err != nil {
		if errors.Is(err, auth.ErrRootRequired) {
			writeAPIError(w, http.StatusForbidden, "root_token_required", "root token required")
			return
		}
		writeAPIError(w, http.StatusInternalServerError, "token_revoke_failed", "could not revoke tokens")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"revoked_count": len(revoked),
		"revoked":       revoked,
	})
}

// handleFeed serves /v1/feed in two modes:
//
//   - Snapshot (back-compat): no cursor, no wait → latest-N events
//     newest-first under {"items": [...]}. Same response shape as v0.
//   - Watcher: cursor and/or wait present → events strictly after the
//     cursor in oldest-first order, with at-least-once semantics. The
//     response gains next_cursor + has_more for resumable consumption.
//
// Mode is selected by the presence of either query param so the v0
// callers (the meristem feed CLI today) keep working without flagging
// their requests, and watcher-mode callers opt in by sending what they
// already need to send anyway. Cursor opacity is contractual — the
// 32-char encoded blob is for round-tripping, not parsing.
func (s *Server) handleFeed(w http.ResponseWriter, r *http.Request) {
	actor, ok := authenticatedToken(w, r)
	if !ok {
		return
	}
	if !s.canReadFeed(w, r, actor) {
		return
	}
	limit, ok := parseLimit(w, r)
	if !ok {
		return
	}
	q := r.URL.Query()
	cursor := q.Get("cursor")
	waitStr := q.Get("wait")

	if cursor == "" && waitStr == "" {
		items, err := s.feed.List(r.Context(), limit)
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, "feed_read_failed", "could not read feed")
			return
		}
		items, err = s.filterFeedItems(r.Context(), actor, items)
		if err != nil {
			writeAccessError(w, err, "token cannot read feed")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": items})
		return
	}

	var wait time.Duration
	if waitStr != "" {
		parsed, err := time.ParseDuration(waitStr)
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, "invalid_wait", "wait must be a Go duration string (e.g. 30s)")
			return
		}
		if parsed < 0 {
			writeAPIError(w, http.StatusBadRequest, "invalid_wait", "wait must be non-negative")
			return
		}
		if parsed > s.policy.MaxFeedWait {
			writeAPIError(w, http.StatusBadRequest, "wait_too_large", fmt.Sprintf("wait must be <= %s", s.policy.MaxFeedWait))
			return
		}
		wait = parsed
	}

	page, err := s.feed.Page(r.Context(), feed.ListOptions{
		Cursor: cursor,
		Wait:   wait,
		Limit:  limit,
	})
	if err != nil {
		if errors.Is(err, feed.ErrInvalidCursor) {
			writeAPIError(w, http.StatusBadRequest, "invalid_cursor", "cursor is malformed; obtain a fresh one from a feed response")
			return
		}
		writeAPIError(w, http.StatusInternalServerError, "feed_read_failed", "could not read feed")
		return
	}
	page, err = s.filterFeedPage(r.Context(), actor, page)
	if err != nil {
		writeAccessError(w, err, "token cannot read feed")
		return
	}
	writeJSON(w, http.StatusOK, page)
}

func (s *Server) handleListWorkItems(w http.ResponseWriter, r *http.Request) {
	actor, ok := authenticatedToken(w, r)
	if !ok {
		return
	}
	if !s.canListWorkItems(w, r, actor) {
		return
	}
	limit, ok := parseLimit(w, r)
	if !ok {
		return
	}
	items, err := s.workItems.List(r.Context(), r.URL.Query().Get("state"), limit)
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, "work_items_read_failed", "could not list work items")
		return
	}
	items, err = s.filterWorkItems(r.Context(), actor, items)
	if err != nil {
		writeAccessError(w, err, "token cannot read work_items")
		return
	}
	out := make([]workItemResponse, 0, len(items))
	for _, item := range items {
		out = append(out, toWorkItemResponse(item))
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": out})
}

func (s *Server) handleBacklogReadiness(w http.ResponseWriter, r *http.Request) {
	actor, ok := authenticatedToken(w, r)
	if !ok {
		return
	}
	if !s.canListWorkItems(w, r, actor) {
		return
	}
	limit, ok := parseReadinessLimit(w, r)
	if !ok {
		return
	}
	items, err := s.workItems.List(r.Context(), "", limit)
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, "backlog_readiness_failed", "could not read backlog readiness")
		return
	}
	items, err = s.filterWorkItems(r.Context(), actor, items)
	if err != nil {
		writeAccessError(w, err, "token cannot read backlog readiness")
		return
	}
	writeJSON(w, http.StatusOK, backlog.Summarize(items, backlog.Options{
		Limit: limit,
		AsOf:  time.Now().UTC(),
	}))
}

func (s *Server) handleCreateWorkItem(w http.ResponseWriter, r *http.Request) {
	actor, ok := authenticatedToken(w, r)
	if !ok {
		return
	}
	var req struct {
		Title                      string   `json:"title"`
		Body                       string   `json:"body"`
		State                      string   `json:"state"`
		SuggestedConvergenceChecks []string `json:"suggested_convergence_checks"`
		HumanReviewStatus          string   `json:"human_review_status"`
	}
	if !decodeJSONRequest(w, r, &req) {
		return
	}
	item, err := s.workItems.Create(r.Context(), workitems.CreateInput{
		Title:                      req.Title,
		Body:                       req.Body,
		State:                      domain.WorkItemState(req.State),
		SuggestedConvergenceChecks: req.SuggestedConvergenceChecks,
		HumanReviewStatus:          domain.HumanReviewStatus(req.HumanReviewStatus),
		Actor:                      actor,
	})
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "work_item_create_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"work_item_id": item.ID,
		"work_item":    toWorkItemResponse(item),
	})
}

func (s *Server) handleGetWorkItem(w http.ResponseWriter, r *http.Request) {
	id, ok := pathUUID(w, r, "id")
	if !ok {
		return
	}
	actor, ok := authenticatedToken(w, r)
	if !ok {
		return
	}
	if !s.canReadWorkItem(w, r, actor, id) {
		return
	}
	item, err := s.workItems.Get(r.Context(), id)
	if err != nil {
		writeWorkItemError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"work_item": toWorkItemResponse(item)})
}

func (s *Server) handleSpawnChild(w http.ResponseWriter, r *http.Request) {
	actor, ok := authenticatedToken(w, r)
	if !ok {
		return
	}
	parentID, ok := pathUUID(w, r, "id")
	if !ok {
		return
	}
	var req struct {
		Title                      string   `json:"title"`
		Body                       string   `json:"body"`
		State                      string   `json:"state"`
		SuggestedConvergenceChecks []string `json:"suggested_convergence_checks"`
		HumanReviewStatus          string   `json:"human_review_status"`
	}
	if !decodeJSONRequest(w, r, &req) {
		return
	}
	item, err := s.workItems.SpawnChild(r.Context(), parentID, workitems.CreateInput{
		Title:                      req.Title,
		Body:                       req.Body,
		State:                      domain.WorkItemState(req.State),
		SuggestedConvergenceChecks: req.SuggestedConvergenceChecks,
		HumanReviewStatus:          domain.HumanReviewStatus(req.HumanReviewStatus),
		Actor:                      actor,
	})
	if err != nil {
		writeWorkItemError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"parent_id":    parentID,
		"work_item_id": item.ID,
		"work_item":    toWorkItemResponse(item),
	})
}

func (s *Server) handleAppendWorkItemEvent(w http.ResponseWriter, r *http.Request) {
	actor, ok := authenticatedToken(w, r)
	if !ok {
		return
	}
	id, ok := pathUUID(w, r, "id")
	if !ok {
		return
	}
	var req struct {
		Kind    string `json:"kind"`
		Payload any    `json:"payload"`
	}
	if !decodeJSONRequest(w, r, &req) {
		return
	}
	if err := s.workItems.AppendEvent(r.Context(), id, req.Kind, req.Payload, actor); err != nil {
		writeWorkItemError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"work_item_id": id, "appended": true})
}

func (s *Server) handleUpdateWorkItemMetadata(w http.ResponseWriter, r *http.Request) {
	actor, ok := authenticatedToken(w, r)
	if !ok {
		return
	}
	id, ok := pathUUID(w, r, "id")
	if !ok {
		return
	}
	var req struct {
		SuggestedConvergenceChecks *[]string `json:"suggested_convergence_checks"`
		HumanReviewStatus          *string   `json:"human_review_status"`
	}
	if !decodeJSONRequest(w, r, &req) {
		return
	}
	if req.SuggestedConvergenceChecks == nil || req.HumanReviewStatus == nil {
		writeAPIError(w, http.StatusBadRequest, "metadata_required", "suggested_convergence_checks and human_review_status are required")
		return
	}
	item, err := s.workItems.UpdateMetadata(r.Context(), id, workitems.UpdateMetadataInput{
		SuggestedConvergenceChecks: *req.SuggestedConvergenceChecks,
		HumanReviewStatus:          domain.HumanReviewStatus(*req.HumanReviewStatus),
		Actor:                      actor,
	})
	if err != nil {
		writeWorkItemError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"work_item": toWorkItemResponse(item)})
}

func (s *Server) handleTransitionWorkItem(w http.ResponseWriter, r *http.Request) {
	actor, ok := authenticatedToken(w, r)
	if !ok {
		return
	}
	id, ok := pathUUID(w, r, "id")
	if !ok {
		return
	}
	var req struct {
		To     string `json:"to"`
		Reason string `json:"reason"`
	}
	if !decodeJSONRequest(w, r, &req) {
		return
	}
	item, err := s.workItems.Transition(r.Context(), id, domain.WorkItemState(req.To), req.Reason, actor)
	if err != nil {
		if strings.Contains(err.Error(), "invalid transition") {
			writeAPIError(w, http.StatusConflict, "invalid_transition", err.Error())
			return
		}
		writeWorkItemError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"work_item": toWorkItemResponse(item)})
}

func authenticatedToken(w http.ResponseWriter, r *http.Request) (domain.Token, bool) {
	tok, ok := auth.TokenFromContext(r.Context())
	if !ok {
		writeAPIError(w, http.StatusUnauthorized, "missing_authenticated_token", "missing authenticated token")
		return domain.Token{}, false
	}
	if !tok.Source.Valid() {
		tok.Source = domain.SourceHuman
	}
	return tok, true
}

func (s *Server) canCaptureInbox(w http.ResponseWriter, r *http.Request) bool {
	actor, ok := authenticatedToken(w, r)
	if !ok {
		return false
	}
	if actor.Source != "" && actor.Source != domain.SourceHuman {
		writeAPIError(w, http.StatusForbidden, "human_token_required", "inbox message capture requires a human token")
		return false
	}
	if !access.ToolVisible(actor, "inbox.capture") {
		writeAPIError(w, http.StatusForbidden, "insufficient_scope", "token cannot capture inbox messages")
		return false
	}
	return true
}

func (s *Server) canPanicRevokeTokens(w http.ResponseWriter, r *http.Request) bool {
	actor, ok := authenticatedToken(w, r)
	if !ok {
		return false
	}
	if !actor.IsRoot {
		writeAPIError(w, http.StatusForbidden, "root_token_required", "root token required")
		return false
	}
	return true
}

func (s *Server) canReadFeed(w http.ResponseWriter, _ *http.Request, actor domain.Token) bool {
	if !access.ToolVisible(actor, "feed.read") {
		writeAPIError(w, http.StatusForbidden, "insufficient_scope", "token cannot read feed")
		return false
	}
	if s.access == nil && access.RequiresScopedPolicy(actor) {
		writeAPIError(w, http.StatusServiceUnavailable, "database_unavailable", "access service is not configured")
		return false
	}
	return true
}

func (s *Server) canCreateWorkItem(w http.ResponseWriter, r *http.Request) bool {
	actor, ok := authenticatedToken(w, r)
	if !ok {
		return false
	}
	if !access.ToolVisible(actor, "work_items.create") {
		writeAPIError(w, http.StatusForbidden, "insufficient_scope", "token cannot create top-level work_items")
		return false
	}
	if s.access == nil {
		if access.RequiresScopedPolicy(actor) {
			writeAPIError(w, http.StatusServiceUnavailable, "database_unavailable", "access service is not configured")
			return false
		}
		return true
	}
	if err := s.access.CanCreateWorkItem(r.Context(), actor); err != nil {
		writeAccessError(w, err, "token cannot create top-level work_items")
		return false
	}
	return true
}

func (s *Server) canListWorkItems(w http.ResponseWriter, _ *http.Request, actor domain.Token) bool {
	if !access.ToolVisible(actor, "work_items.list") {
		writeAPIError(w, http.StatusForbidden, "insufficient_scope", "token cannot read work_items")
		return false
	}
	if s.access == nil && access.RequiresScopedPolicy(actor) {
		writeAPIError(w, http.StatusServiceUnavailable, "database_unavailable", "access service is not configured")
		return false
	}
	return true
}

func (s *Server) canReadWorkItem(w http.ResponseWriter, r *http.Request, actor domain.Token, id uuid.UUID) bool {
	if !access.ToolVisible(actor, "work_items.get") {
		writeAPIError(w, http.StatusForbidden, "insufficient_scope", "token cannot read work_items")
		return false
	}
	if s.access == nil {
		if access.RequiresScopedPolicy(actor) {
			writeAPIError(w, http.StatusServiceUnavailable, "database_unavailable", "access service is not configured")
			return false
		}
		return true
	}
	if err := s.access.CanReadWorkItem(r.Context(), actor, id); err != nil {
		if errors.Is(err, access.ErrDenied) {
			writeAPIError(w, http.StatusNotFound, "work_item_not_found", "work item not found")
			return false
		}
		writeAccessError(w, err, "token cannot read work_items")
		return false
	}
	return true
}

func (s *Server) canWriteWorkItemPath(tool string) accessGate {
	return func(w http.ResponseWriter, r *http.Request) bool {
		actor, ok := authenticatedToken(w, r)
		if !ok {
			return false
		}
		if !access.ToolVisible(actor, tool) {
			writeAPIError(w, http.StatusForbidden, "insufficient_scope", "token cannot write work_items")
			return false
		}
		id, ok := pathUUID(w, r, "id")
		if !ok {
			return false
		}
		if s.access == nil {
			if access.RequiresScopedPolicy(actor) {
				writeAPIError(w, http.StatusServiceUnavailable, "database_unavailable", "access service is not configured")
				return false
			}
			return true
		}
		if err := s.access.CanWriteWorkItem(r.Context(), actor, id); err != nil {
			if errors.Is(err, access.ErrDenied) {
				writeAPIError(w, http.StatusNotFound, "work_item_not_found", "work item not found")
				return false
			}
			writeAccessError(w, err, "token cannot write work_items")
			return false
		}
		return true
	}
}

func (s *Server) filterWorkItems(ctx context.Context, actor domain.Token, items []domain.WorkItem) ([]domain.WorkItem, error) {
	if s.access == nil {
		if access.RequiresScopedPolicy(actor) {
			return nil, fmt.Errorf("access service is not configured")
		}
		return items, nil
	}
	return s.access.FilterWorkItems(ctx, actor, items)
}

func (s *Server) filterFeedItems(ctx context.Context, actor domain.Token, items []feed.Item) ([]feed.Item, error) {
	if s.access == nil {
		if access.RequiresScopedPolicy(actor) {
			return nil, fmt.Errorf("access service is not configured")
		}
		return items, nil
	}
	return s.access.FilterFeedItems(ctx, actor, items)
}

func (s *Server) filterFeedPage(ctx context.Context, actor domain.Token, page feed.Page) (feed.Page, error) {
	if s.access == nil {
		if access.RequiresScopedPolicy(actor) {
			return feed.Page{}, fmt.Errorf("access service is not configured")
		}
		return page, nil
	}
	return s.access.FilterFeedPage(ctx, actor, page)
}

func writeAccessError(w http.ResponseWriter, err error, message string) {
	if errors.Is(err, access.ErrDenied) {
		writeAPIError(w, http.StatusForbidden, "insufficient_scope", message)
		return
	}
	writeAPIError(w, http.StatusInternalServerError, "access_check_failed", "could not evaluate access policy")
}

func decodeJSONRequest(w http.ResponseWriter, r *http.Request, out any) bool {
	defer func() { _ = r.Body.Close() }()
	r.Body = http.MaxBytesReader(w, r.Body, safety.DefaultPolicy().MaxRequestBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.UseNumber()
	if err := dec.Decode(out); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeAPIError(w, http.StatusRequestEntityTooLarge, "request_too_large", "request body exceeds resource safety limit")
			return false
		}
		writeAPIError(w, http.StatusBadRequest, "invalid_json", "request body must be valid JSON")
		return false
	}
	return true
}

func parseLimit(w http.ResponseWriter, r *http.Request) (int, bool) {
	raw := r.URL.Query().Get("limit")
	if raw == "" {
		return 50, true
	}
	limit, err := strconv.Atoi(raw)
	if err != nil || limit < 0 {
		writeAPIError(w, http.StatusBadRequest, "invalid_limit", "limit must be a non-negative integer")
		return 0, false
	}
	return limit, true
}

func parseReadinessLimit(w http.ResponseWriter, r *http.Request) (int, bool) {
	raw := r.URL.Query().Get("limit")
	if raw == "" {
		return 200, true
	}
	limit, err := strconv.Atoi(raw)
	if err != nil || limit < 0 {
		writeAPIError(w, http.StatusBadRequest, "invalid_limit", "limit must be a non-negative integer")
		return 0, false
	}
	if limit == 0 {
		return 200, true
	}
	if limit > 200 {
		writeAPIError(w, http.StatusBadRequest, "invalid_limit", "limit must be <= 200")
		return 0, false
	}
	return limit, true
}

func pathUUID(w http.ResponseWriter, r *http.Request, name string) (uuid.UUID, bool) {
	id, err := uuid.Parse(r.PathValue(name))
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_uuid", fmt.Sprintf("%s must be a valid uuid", name))
		return uuid.Nil, false
	}
	return id, true
}

func toWorkItemResponse(item domain.WorkItem) workItemResponse {
	return workItemResponse{
		ID:                         item.ID,
		Title:                      item.Title,
		Body:                       item.Body,
		State:                      item.State,
		StateReason:                item.StateReason,
		SuggestedConvergenceChecks: item.SuggestedConvergenceChecks,
		HumanReviewStatus:          item.HumanReviewStatus,
		CreatedBy:                  item.CreatedBy,
		CreatedAt:                  item.CreatedAt,
		StateEnteredAt:             item.StateEnteredAt,
		UpdatedAt:                  item.UpdatedAt,
	}
}

func toSubactorGrantResponse(result grants.IssueResult) subactorGrantResponse {
	resp := subactorGrantResponse{
		GrantID:     result.GrantID,
		WorkItemID:  result.WorkItemID,
		Template:    result.Template,
		Disposition: result.Disposition,
		Reason:      result.Reason,
		Scopes:      result.Scopes,
		TokenSecret: result.TokenSecret,
		Events: subactorGrantEvents{
			Requested: result.RequestEventID,
			Outcome:   result.OutcomeEventID,
		},
	}
	if result.Token != nil {
		resp.Token = &subactorGrantToken{
			ID:     result.Token.ID,
			Name:   result.Token.Name,
			Source: result.Token.Source,
			Scopes: result.Token.Scopes,
		}
	}
	if result.EscalationID != uuid.Nil {
		resp.Escalation = &subactorGrantEscalation{
			ID:              result.EscalationID,
			HumanWorkItemID: result.HumanWorkItemID,
		}
	}
	return resp
}

func writeGrantError(w http.ResponseWriter, err error) {
	if errors.Is(err, grants.ErrWorkItemNotFound) {
		writeAPIError(w, http.StatusNotFound, "work_item_not_found", "work item not found")
		return
	}
	if strings.Contains(err.Error(), "required") || strings.Contains(err.Error(), "blank") || strings.Contains(err.Error(), "invalid human_review_status") {
		writeAPIError(w, http.StatusBadRequest, "subactor_grant_request_failed", err.Error())
		return
	}
	writeAPIError(w, http.StatusInternalServerError, "subactor_grant_failed", "could not issue subactor grant")
}

func writeWorkItemError(w http.ResponseWriter, err error) {
	if errors.Is(err, workitems.ErrNotFound) {
		writeAPIError(w, http.StatusNotFound, "work_item_not_found", "work item not found")
		return
	}
	if errors.Is(err, workitems.ErrRelationCycle) {
		writeAPIError(w, http.StatusConflict, "relation_cycle", err.Error())
		return
	}
	if strings.Contains(err.Error(), "invalid state") {
		writeAPIError(w, http.StatusBadRequest, "invalid_state", err.Error())
		return
	}
	writeAPIError(w, http.StatusBadRequest, "work_item_request_failed", err.Error())
}

func writeAPIError(w http.ResponseWriter, status int, code string, message string) {
	writeJSON(w, status, map[string]any{
		"error": map[string]string{
			"code":    code,
			"message": message,
		},
	})
}
