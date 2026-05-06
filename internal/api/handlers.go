package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/feed"
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
	UpdatedAt                  time.Time                `json:"updated_at"`
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
	writeJSON(w, http.StatusOK, page)
}

func (s *Server) handleListWorkItems(w http.ResponseWriter, r *http.Request) {
	limit, ok := parseLimit(w, r)
	if !ok {
		return
	}
	items, err := s.workItems.List(r.Context(), r.URL.Query().Get("state"), limit)
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, "work_items_read_failed", "could not list work items")
		return
	}
	out := make([]workItemResponse, 0, len(items))
	for _, item := range items {
		out = append(out, toWorkItemResponse(item))
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": out})
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
		UpdatedAt:                  item.UpdatedAt,
	}
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
