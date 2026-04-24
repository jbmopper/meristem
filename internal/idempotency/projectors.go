package idempotency

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/jbmopper/wayline/internal/domain"
	"github.com/jbmopper/wayline/internal/projections"
)

func RegisterProjectors(registry *projections.Registry) {
	registry.Register(recordedProjector{})
}

type recordedProjector struct{}

func (recordedProjector) Kind() string { return domain.EventIdempotencyRecorded }

func (recordedProjector) Apply(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	var payload struct {
		TokenID        uuid.UUID       `json:"token_id"`
		Scope          string          `json:"scope"`
		Key            string          `json:"key"`
		RequestHash    string          `json:"request_hash"`
		ResponseStatus int             `json:"response_status"`
		ResponseBody   json.RawMessage `json:"response_body"`
		ExpiresAt      time.Time       `json:"expires_at"`
	}
	b, err := json.Marshal(event.Payload)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(b, &payload); err != nil {
		return err
	}
	if payload.TokenID == uuid.Nil || payload.Scope == "" || payload.Key == "" {
		return fmt.Errorf("idempotency.recorded: token_id, scope and key are required")
	}
	requestHash, err := base64.StdEncoding.DecodeString(payload.RequestHash)
	if err != nil {
		return fmt.Errorf("idempotency.recorded: decode request hash: %w", err)
	}
	if len(payload.ResponseBody) == 0 {
		payload.ResponseBody = []byte(`{}`)
	}
	if payload.ExpiresAt.IsZero() {
		payload.ExpiresAt = event.OccurredAt.Add(ttl)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO idempotency_keys
			(token_id, scope, key, request_hash, response_status, response_body, created_at, expires_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (token_id, scope, key) DO UPDATE SET
			request_hash = EXCLUDED.request_hash,
			response_status = EXCLUDED.response_status,
			response_body = EXCLUDED.response_body,
			expires_at = EXCLUDED.expires_at
	`, payload.TokenID, payload.Scope, payload.Key, requestHash, payload.ResponseStatus, payload.ResponseBody, event.OccurredAt, payload.ExpiresAt)
	return err
}
