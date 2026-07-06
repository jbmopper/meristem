// Package httpconnector is the proof connector for approval-gated side
// effects. It deliberately avoids credentials, connection catalogs, and broad
// connector abstractions: the only write path is approval -> outbox -> dispatch.
package httpconnector

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/approvals"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/idempotency"
)

var (
	ErrActorRequired      = errors.New("httpconnector: actor token is required")
	ErrNotFound           = errors.New("httpconnector: action not found")
	ErrInvalidMode        = errors.New("httpconnector: invalid mode")
	ErrInvalidMethod      = errors.New("httpconnector: invalid method")
	ErrInvalidURL         = errors.New("httpconnector: invalid url")
	ErrApprovalRequired   = errors.New("httpconnector: approved approval is required before dispatch")
	ErrNoPendingOutbox    = errors.New("httpconnector: no pending outbox event")
	ErrUnsupportedRequest = errors.New("httpconnector: unsupported request")
)

type Mode string

const (
	ModeRead  Mode = "read"
	ModeWrite Mode = "write"
)

type Status string

const (
	StatusRequested        Status = "requested"
	StatusAwaitingApproval Status = "awaiting_approval"
	StatusApproved         Status = "approved"
	StatusSent             Status = "sent"
	StatusFailed           Status = "failed"
)

type Action struct {
	ID             uuid.UUID       `json:"id"`
	WorkItemID     uuid.UUID       `json:"work_item_id"`
	Mode           Mode            `json:"mode"`
	Method         string          `json:"method"`
	URL            string          `json:"url"`
	Request        json.RawMessage `json:"request"`
	Status         Status          `json:"status"`
	ApprovalID     *uuid.UUID      `json:"approval_id,omitempty"`
	ResponseStatus *int            `json:"response_status,omitempty"`
	ResponseBody   string          `json:"response_body"`
	Error          string          `json:"error"`
	RequestedBy    *uuid.UUID      `json:"requested_by,omitempty"`
	Source         domain.Source   `json:"source"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
}

type RequestInput struct {
	ActionID   uuid.UUID
	ApprovalID uuid.UUID
	WorkItemID uuid.UUID
	Mode       Mode
	Method     string
	URL        string
	Body       json.RawMessage
	Actor      domain.Token
}

type RequestResult struct {
	Action        Action
	Approval      *approvals.Approval
	Fresh         bool
	EventID       uuid.UUID
	ApprovalEvent uuid.UUID
}

type EnqueueResult struct {
	Action  Action
	Fresh   bool
	EventID uuid.UUID
}

type DispatchResult struct {
	Action        Action
	OutboxEventID uuid.UUID
	HTTPStatus    int
	HTTPBody      string
	Dispatched    bool
}

type HTTPDoer interface {
	Do(*http.Request) (*http.Response, error)
}

type Service struct {
	pool      *pgxpool.Pool
	writer    *events.Writer
	approvals *approvals.Service
	client    HTTPDoer
	clock     func() time.Time
}

func NewService(pool *pgxpool.Pool, writer *events.Writer, approvalSvc *approvals.Service, client HTTPDoer) *Service {
	return NewServiceWithClock(pool, writer, approvalSvc, client, nil)
}

func NewServiceWithClock(pool *pgxpool.Pool, writer *events.Writer, approvalSvc *approvals.Service, client HTTPDoer, clock func() time.Time) *Service {
	if approvalSvc == nil {
		approvalSvc = approvals.NewService(pool, writer)
	}
	if client == nil {
		client = http.DefaultClient
	}
	if clock == nil {
		clock = time.Now
	}
	return &Service{pool: pool, writer: writer, approvals: approvalSvc, client: client, clock: clock}
}

func (s *Service) Request(ctx context.Context, in RequestInput) (RequestResult, error) {
	if in.Actor.ID == uuid.Nil {
		return RequestResult{}, ErrActorRequired
	}
	if in.WorkItemID == uuid.Nil {
		return RequestResult{}, fmt.Errorf("%w: work_item_id is required", ErrUnsupportedRequest)
	}
	normalized, err := normalizeRequest(in)
	if err != nil {
		return RequestResult{}, err
	}
	actionID := in.ActionID
	if actionID == uuid.Nil {
		actionID = newSubjectID(ctx, "http_connector_action")
	}
	approvalID := in.ApprovalID
	if normalized.Mode == ModeWrite && approvalID == uuid.Nil {
		approvalID = newSubjectID(ctx, "approval")
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return RequestResult{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := lockWorkItem(ctx, tx, normalized.WorkItemID); err != nil {
		return RequestResult{}, err
	}
	var approvalEvent uuid.UUID
	if normalized.Mode == ModeWrite {
		request := map[string]any{
			"connector": "http",
			"action_id": actionID,
			"method":    normalized.Method,
			"url":       normalized.URL,
			"request":   normalized.Request,
		}
		_, approvalEvent, _, err = s.approvals.CreateInTx(ctx, tx, approvals.CreateInput{
			ApprovalID: approvalID,
			WorkItemID: normalized.WorkItemID,
			Summary:    "HTTP connector write " + normalized.Method + " " + normalized.URL,
			Request:    request,
			Actor:      in.Actor,
		})
		if err != nil {
			return RequestResult{}, err
		}
	}
	payload := requestEventPayload(normalized, approvalID)
	eventID, fresh, err := s.writer.Append(ctx, tx, events.Spec{
		SubjectKind:  domain.SubjectHTTPConnectorAction,
		SubjectID:    actionID,
		Kind:         domain.EventHTTPConnectorActionRequested,
		Source:       sourceForActor(in.Actor),
		ActorTokenID: &in.Actor.ID,
		Payload:      payload,
	})
	if err != nil {
		return RequestResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return RequestResult{}, err
	}

	action, err := s.Get(ctx, actionID)
	if err != nil {
		return RequestResult{}, err
	}
	var approval *approvals.Approval
	if action.ApprovalID != nil {
		item, err := s.approvals.Get(ctx, *action.ApprovalID)
		if err != nil {
			return RequestResult{}, err
		}
		approval = &item
	}
	if action.Mode == ModeRead {
		sent, err := s.executeAction(ctx, action, in.Actor)
		if err != nil {
			return RequestResult{}, err
		}
		action = sent.Action
	}
	return RequestResult{Action: action, Approval: approval, Fresh: fresh, EventID: eventID, ApprovalEvent: approvalEvent}, nil
}

func (s *Service) EnqueueApprovedWrite(ctx context.Context, actionID uuid.UUID, actor domain.Token) (EnqueueResult, error) {
	if actor.ID == uuid.Nil {
		return EnqueueResult{}, ErrActorRequired
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return EnqueueResult{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	action, err := scanActionForUpdate(ctx, tx, actionID)
	if err != nil {
		return EnqueueResult{}, err
	}
	if action.Mode != ModeWrite {
		return EnqueueResult{}, ErrInvalidMode
	}
	if action.Status == StatusSent || action.Status == StatusApproved {
		if err := tx.Commit(ctx); err != nil {
			return EnqueueResult{}, err
		}
		return EnqueueResult{Action: action, Fresh: false}, nil
	}
	if action.ApprovalID == nil {
		return EnqueueResult{}, ErrApprovalRequired
	}
	approval, err := s.approvals.Get(ctx, *action.ApprovalID)
	if err != nil {
		return EnqueueResult{}, err
	}
	if approval.Status != approvals.StatusApproved {
		return EnqueueResult{}, ErrApprovalRequired
	}
	eventID, fresh, err := s.writer.Append(ctx, tx, events.Spec{
		SubjectKind:  domain.SubjectHTTPConnectorAction,
		SubjectID:    action.ID,
		Kind:         domain.EventHTTPConnectorActionApproved,
		Source:       sourceForActor(actor),
		ActorTokenID: &actor.ID,
		Payload: map[string]any{
			"work_item_id": action.WorkItemID,
			"approval_id":  *action.ApprovalID,
			"method":       action.Method,
			"url":          action.URL,
			"request":      rawJSON(action.Request),
		},
	})
	if err != nil {
		return EnqueueResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return EnqueueResult{}, err
	}
	updated, err := s.Get(ctx, action.ID)
	if err != nil {
		return EnqueueResult{}, err
	}
	return EnqueueResult{Action: updated, Fresh: fresh, EventID: eventID}, nil
}

func (s *Service) DispatchOnce(ctx context.Context, actor domain.Token, lease time.Duration) (DispatchResult, error) {
	if actor.ID == uuid.Nil {
		return DispatchResult{}, ErrActorRequired
	}
	if lease <= 0 {
		lease = time.Minute
	}
	outbox, found, err := s.claimOutbox(ctx, lease)
	if err != nil {
		return DispatchResult{}, err
	}
	if !found {
		return DispatchResult{}, ErrNoPendingOutbox
	}
	action, err := s.Get(ctx, outbox.ActionID)
	if err != nil {
		return DispatchResult{}, err
	}
	sent, err := s.executeAction(ctx, action, actor)
	if err != nil {
		return DispatchResult{}, err
	}
	if err := s.markOutboxSent(ctx, outbox.ID); err != nil {
		return DispatchResult{}, err
	}
	return DispatchResult{
		Action:        sent.Action,
		OutboxEventID: outbox.ID,
		HTTPStatus:    sent.HTTPStatus,
		HTTPBody:      sent.HTTPBody,
		Dispatched:    true,
	}, nil
}

func (s *Service) Get(ctx context.Context, id uuid.UUID) (Action, error) {
	row := s.pool.QueryRow(ctx, actionSelectSQL+` WHERE id = $1`, id)
	return scanAction(row)
}

type sentResult struct {
	Action     Action
	HTTPStatus int
	HTTPBody   string
}

func (s *Service) executeAction(ctx context.Context, action Action, actor domain.Token) (sentResult, error) {
	var body io.Reader
	if len(action.Request) > 0 && string(action.Request) != "null" && string(action.Request) != "{}" {
		var payload struct {
			Body json.RawMessage `json:"body"`
		}
		if err := json.Unmarshal(action.Request, &payload); err != nil {
			return sentResult{}, fmt.Errorf("httpconnector: decode request: %w", err)
		}
		if len(payload.Body) > 0 && string(payload.Body) != "null" {
			body = bytes.NewReader(payload.Body)
		}
	}
	req, err := http.NewRequestWithContext(ctx, action.Method, action.URL, body)
	if err != nil {
		return sentResult{}, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return sentResult{}, err
	}
	defer resp.Body.Close()
	rawBody, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if err != nil {
		return sentResult{}, err
	}
	bodyText := string(rawBody)
	eventID, _, err := s.appendSent(ctx, action, actor, resp.StatusCode, bodyText)
	if err != nil {
		return sentResult{}, err
	}
	_ = eventID
	updated, err := s.Get(ctx, action.ID)
	if err != nil {
		return sentResult{}, err
	}
	return sentResult{Action: updated, HTTPStatus: resp.StatusCode, HTTPBody: bodyText}, nil
}

func (s *Service) appendSent(ctx context.Context, action Action, actor domain.Token, status int, body string) (uuid.UUID, bool, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return uuid.Nil, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	eventID, fresh, err := s.writer.Append(ctx, tx, events.Spec{
		SubjectKind:   domain.SubjectHTTPConnectorAction,
		SubjectID:     action.ID,
		Kind:          domain.EventHTTPConnectorActionSent,
		Source:        sourceForActor(actor),
		ActorTokenID:  &actor.ID,
		Discriminator: action.ID.String(),
		Payload: map[string]any{
			"work_item_id":    action.WorkItemID,
			"response_status": status,
			"response_body":   body,
		},
	})
	if err != nil {
		return uuid.Nil, false, err
	}
	return eventID, fresh, tx.Commit(ctx)
}

type normalizedRequest struct {
	WorkItemID uuid.UUID
	Mode       Mode
	Method     string
	URL        string
	Request    json.RawMessage
}

func normalizeRequest(in RequestInput) (normalizedRequest, error) {
	mode := Mode(strings.ToLower(strings.TrimSpace(string(in.Mode))))
	if mode != ModeRead && mode != ModeWrite {
		return normalizedRequest{}, ErrInvalidMode
	}
	method := strings.ToUpper(strings.TrimSpace(in.Method))
	if method == "" {
		if mode == ModeRead {
			method = http.MethodGet
		} else {
			method = http.MethodPost
		}
	}
	if mode == ModeRead && method != http.MethodGet && method != http.MethodHead {
		return normalizedRequest{}, fmt.Errorf("%w: read mode accepts GET or HEAD", ErrInvalidMethod)
	}
	if mode == ModeWrite {
		switch method {
		case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		default:
			return normalizedRequest{}, fmt.Errorf("%w: write mode accepts POST, PUT, PATCH, or DELETE", ErrInvalidMethod)
		}
	}
	parsed, err := url.Parse(strings.TrimSpace(in.URL))
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return normalizedRequest{}, ErrInvalidURL
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return normalizedRequest{}, ErrInvalidURL
	}
	request := json.RawMessage(`{}`)
	if len(in.Body) > 0 && string(in.Body) != "null" {
		requestBody, err := json.Marshal(map[string]json.RawMessage{"body": in.Body})
		if err != nil {
			return normalizedRequest{}, err
		}
		request = requestBody
	}
	return normalizedRequest{
		WorkItemID: in.WorkItemID,
		Mode:       mode,
		Method:     method,
		URL:        parsed.String(),
		Request:    request,
	}, nil
}

func requestEventPayload(in normalizedRequest, approvalID uuid.UUID) map[string]any {
	payload := map[string]any{
		"work_item_id": in.WorkItemID,
		"mode":         string(in.Mode),
		"method":       in.Method,
		"url":          in.URL,
		"request":      rawJSON(in.Request),
	}
	if approvalID != uuid.Nil {
		payload["approval_id"] = approvalID
	}
	return payload
}

func lockWorkItem(ctx context.Context, tx pgx.Tx, id uuid.UUID) error {
	var terminal bool
	if err := tx.QueryRow(ctx, `SELECT state IN ('done', 'failed', 'canceled') FROM work_items WHERE id = $1 FOR UPDATE`, id).Scan(&terminal); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		return err
	}
	if terminal {
		return fmt.Errorf("%w: terminal work item", ErrUnsupportedRequest)
	}
	return nil
}

type outboxRow struct {
	ID       uuid.UUID
	ActionID uuid.UUID
}

func (s *Service) claimOutbox(ctx context.Context, lease time.Duration) (outboxRow, bool, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return outboxRow{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	leaseMillis := lease.Milliseconds()
	if leaseMillis <= 0 {
		leaseMillis = 1
	}
	var row outboxRow
	err = tx.QueryRow(ctx, `
		WITH next AS (
			SELECT id
			FROM outbox_events
			WHERE kind = 'http_connector.write'
			  AND (
			    state = 'pending'
			    OR (state = 'leased' AND lease_until <= now())
			  )
			ORDER BY created_at ASC, id ASC
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		UPDATE outbox_events oe
		SET state = 'leased',
		    attempts = attempts + 1,
		    lease_until = now() + ($1::bigint * interval '1 millisecond'),
		    updated_at = now()
		FROM next
		WHERE oe.id = next.id
		RETURNING oe.id, oe.action_id
	`, leaseMillis).Scan(&row.ID, &row.ActionID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return outboxRow{}, false, tx.Commit(ctx)
		}
		return outboxRow{}, false, err
	}
	return row, true, tx.Commit(ctx)
}

func (s *Service) markOutboxSent(ctx context.Context, id uuid.UUID) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE outbox_events
		SET state = 'sent', lease_until = NULL, updated_at = now()
		WHERE id = $1
	`, id)
	return err
}

const actionSelectSQL = `
	SELECT id, work_item_id, mode, method, url, request, status, approval_id,
	       response_status, response_body, error, requested_by, source, created_at, updated_at
	FROM http_connector_actions`

func scanActionForUpdate(ctx context.Context, tx pgx.Tx, id uuid.UUID) (Action, error) {
	return scanAction(tx.QueryRow(ctx, actionSelectSQL+` WHERE id = $1 FOR UPDATE`, id))
}

func scanAction(row pgx.Row) (Action, error) {
	var (
		item           Action
		mode           string
		status         string
		approvalID     uuid.NullUUID
		responseStatus sql.NullInt32
		requestedBy    uuid.NullUUID
		source         string
	)
	if err := row.Scan(
		&item.ID,
		&item.WorkItemID,
		&mode,
		&item.Method,
		&item.URL,
		&item.Request,
		&status,
		&approvalID,
		&responseStatus,
		&item.ResponseBody,
		&item.Error,
		&requestedBy,
		&source,
		&item.CreatedAt,
		&item.UpdatedAt,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Action{}, ErrNotFound
		}
		return Action{}, err
	}
	item.Mode = Mode(mode)
	item.Status = Status(status)
	if approvalID.Valid {
		id := approvalID.UUID
		item.ApprovalID = &id
	}
	if responseStatus.Valid {
		v := int(responseStatus.Int32)
		item.ResponseStatus = &v
	}
	if requestedBy.Valid {
		id := requestedBy.UUID
		item.RequestedBy = &id
	}
	item.Source = domain.Source(source)
	if len(item.Request) == 0 {
		item.Request = json.RawMessage(`{}`)
	}
	return item, nil
}

func rawJSON(raw json.RawMessage) any {
	if len(raw) == 0 || string(raw) == "null" {
		return map[string]any{}
	}
	var out any
	if err := json.Unmarshal(raw, &out); err != nil {
		return map[string]any{}
	}
	return out
}

func sourceForActor(actor domain.Token) domain.Source {
	if actor.Source.Valid() {
		return actor.Source
	}
	return domain.SourceHuman
}

func newSubjectID(ctx context.Context, label string) uuid.UUID {
	if id, ok := idempotency.SubjectID(ctx, label); ok {
		return id
	}
	return uuid.New()
}
