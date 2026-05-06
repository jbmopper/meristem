// Package errorreporting records deterministic-layer errors as event-sourced,
// maskable reports.
//
// The deterministic layer is the code that validates, reconciles, projects,
// and persists facts without model judgment. Its failures need a durable
// operator-facing surface, but masking is only display policy: events remain
// immutable and replayable.
package errorreporting

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/idempotency"
)

var (
	ErrNotFound          = errors.New("errorreporting: deterministic error not found")
	ErrComponentRequired = errors.New("errorreporting: component is required")
	ErrCodeRequired      = errors.New("errorreporting: code is required")
	ErrMessageRequired   = errors.New("errorreporting: message is required")
	ErrInvalidSeverity   = errors.New("errorreporting: invalid severity")
	ErrInvalidDetails    = errors.New("errorreporting: details must be a JSON object")
	ErrAccessDenied      = errors.New("errorreporting: access denied")
)

// Service is the deterministic error-report write/read surface. All state
// changes are event appends; reads come from the deterministic_errors
// projection.
type Service struct {
	pool   *pgxpool.Pool
	writer *events.Writer
}

func NewService(pool *pgxpool.Pool, writer *events.Writer) *Service {
	return &Service{pool: pool, writer: writer}
}

type ReportInput struct {
	Component string
	Code      string
	Message   string
	Severity  domain.DeterministicErrorSeverity
	Details   json.RawMessage
	Actor     domain.Token
}

type MaskInput struct {
	Reason string
	Actor  domain.Token
}

type ListOptions struct {
	IncludeMasked bool
	Limit         int
}

func (s *Service) Report(ctx context.Context, in ReportInput) (domain.DeterministicError, error) {
	component := strings.TrimSpace(in.Component)
	code := strings.TrimSpace(in.Code)
	message := strings.TrimSpace(in.Message)
	severity := in.Severity
	if severity == "" {
		severity = domain.DeterministicErrorError
	}
	details, err := normalizeDetails(in.Details)
	if err != nil {
		return domain.DeterministicError{}, err
	}
	switch {
	case component == "":
		return domain.DeterministicError{}, ErrComponentRequired
	case code == "":
		return domain.DeterministicError{}, ErrCodeRequired
	case message == "":
		return domain.DeterministicError{}, ErrMessageRequired
	case !severity.Valid():
		return domain.DeterministicError{}, fmt.Errorf("%w: %q", ErrInvalidSeverity, severity)
	}

	id := newSubjectID(ctx, "deterministic_error")
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return domain.DeterministicError{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, _, err := s.writer.Append(ctx, tx, events.Spec{
		SubjectKind:  domain.SubjectDeterministicError,
		SubjectID:    id,
		Kind:         domain.EventDeterministicErrorReported,
		Source:       sourceForActor(in.Actor),
		ActorTokenID: &in.Actor.ID,
		Payload: map[string]any{
			"component": component,
			"code":      code,
			"message":   message,
			"severity":  severity,
			"details":   json.RawMessage(details),
		},
	}); err != nil {
		return domain.DeterministicError{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.DeterministicError{}, err
	}
	return s.Get(ctx, id)
}

func (s *Service) Mask(ctx context.Context, id uuid.UUID, in MaskInput) (domain.DeterministicError, error) {
	return s.setMasked(ctx, id, true, in)
}

func (s *Service) Unmask(ctx context.Context, id uuid.UUID, in MaskInput) (domain.DeterministicError, error) {
	return s.setMasked(ctx, id, false, in)
}

func (s *Service) setMasked(ctx context.Context, id uuid.UUID, masked bool, in MaskInput) (domain.DeterministicError, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return domain.DeterministicError{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := scanDeterministicError(ctx, tx, id); err != nil {
		return domain.DeterministicError{}, err
	}

	kind := domain.EventDeterministicErrorUnmasked
	if masked {
		kind = domain.EventDeterministicErrorMasked
	}
	if _, _, err := s.writer.Append(ctx, tx, events.Spec{
		SubjectKind:  domain.SubjectDeterministicError,
		SubjectID:    id,
		Kind:         kind,
		Source:       sourceForActor(in.Actor),
		ActorTokenID: &in.Actor.ID,
		Payload: map[string]any{
			"reason": strings.TrimSpace(in.Reason),
		},
	}); err != nil {
		return domain.DeterministicError{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.DeterministicError{}, err
	}
	return s.Get(ctx, id)
}

func (s *Service) Get(ctx context.Context, id uuid.UUID) (domain.DeterministicError, error) {
	return scanDeterministicError(ctx, s.pool, id)
}

func (s *Service) GetForAccessor(ctx context.Context, id uuid.UUID, accessor domain.Token) (domain.DeterministicError, error) {
	policy := PolicyForToken(accessor)
	if !policy.CanRead {
		return domain.DeterministicError{}, ErrAccessDenied
	}
	item, err := s.Get(ctx, id)
	if err != nil {
		return domain.DeterministicError{}, err
	}
	if item.Masked && !policy.CanReadMasked {
		return domain.DeterministicError{}, ErrNotFound
	}
	return policy.Filter(item), nil
}

func (s *Service) List(ctx context.Context, opts ListOptions) ([]domain.DeterministicError, error) {
	limit := opts.Limit
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id, component, code, message, severity, details, reported_by,
		       reported_at, updated_at, masked, mask_reason, masked_by, masked_at
		FROM deterministic_errors
		WHERE ($2::boolean OR masked = FALSE)
		ORDER BY updated_at DESC
		LIMIT $1
	`, limit, opts.IncludeMasked)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.DeterministicError
	for rows.Next() {
		item, err := scanDeterministicErrorRow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *Service) ListForAccessor(ctx context.Context, opts ListOptions, accessor domain.Token) ([]domain.DeterministicError, error) {
	policy := PolicyForToken(accessor)
	if !policy.CanRead {
		return nil, ErrAccessDenied
	}
	if opts.IncludeMasked && !policy.CanReadMasked {
		return nil, ErrAccessDenied
	}
	items, err := s.List(ctx, opts)
	if err != nil {
		return nil, err
	}
	for i := range items {
		items[i] = policy.Filter(items[i])
	}
	return items, nil
}

type queryer interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanDeterministicError(ctx context.Context, q queryer, id uuid.UUID) (domain.DeterministicError, error) {
	return scanDeterministicErrorRow(q.QueryRow(ctx, `
		SELECT id, component, code, message, severity, details, reported_by,
		       reported_at, updated_at, masked, mask_reason, masked_by, masked_at
		FROM deterministic_errors
		WHERE id = $1
	`, id))
}

func scanDeterministicErrorRow(row rowScanner) (domain.DeterministicError, error) {
	var (
		out      domain.DeterministicError
		severity string
		details  []byte
	)
	if err := row.Scan(
		&out.ID, &out.Component, &out.Code, &out.Message, &severity, &details,
		&out.ReportedBy, &out.ReportedAt, &out.UpdatedAt, &out.Masked,
		&out.MaskReason, &out.MaskedBy, &out.MaskedAt,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.DeterministicError{}, ErrNotFound
		}
		return domain.DeterministicError{}, err
	}
	out.Severity = domain.DeterministicErrorSeverity(severity)
	out.Details = details
	return out, nil
}

func normalizeDetails(raw json.RawMessage) (json.RawMessage, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return json.RawMessage(`{}`), nil
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidDetails, err)
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, ErrInvalidDetails
	}
	if _, ok := v.(map[string]any); !ok {
		return nil, ErrInvalidDetails
	}
	return raw, nil
}

func sourceForActor(actor domain.Token) domain.Source {
	if actor.Source.Valid() {
		return actor.Source
	}
	return domain.SourceSystem
}

func newSubjectID(ctx context.Context, label string) uuid.UUID {
	if id, ok := idempotency.SubjectID(ctx, label); ok {
		return id
	}
	return uuid.New()
}
