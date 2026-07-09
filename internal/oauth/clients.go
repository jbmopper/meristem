package oauth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrClientNotFound is returned when no oauth_clients row matches a client_id.
var ErrClientNotFound = errors.New("oauth: client not found")

// Client is the read model for a registered OAuth client, folded from
// oauth_client.registered events. It carries no secret material.
type Client struct {
	ClientID                string
	ClientName              string
	RedirectURIs            []string
	GrantTypes              []string
	ResponseTypes           []string
	TokenEndpointAuthMethod string
	Scope                   string
	CreatedAt               time.Time
	UpdatedAt               time.Time
}

// AllowsRedirectURI reports whether uri is in the client's registered
// allowlist by exact string match (RFC 7591/8252: no wildcard, no prefix).
func (c Client) AllowsRedirectURI(uri string) bool {
	for _, r := range c.RedirectURIs {
		if r == uri {
			return true
		}
	}
	return false
}

// GetClient loads one registered client from the oauth_clients projection.
func GetClient(ctx context.Context, pool *pgxpool.Pool, clientID string) (Client, error) {
	if pool == nil {
		return Client{}, errors.New("oauth: pool is required")
	}
	var (
		c                                    Client
		redirectsJSON, grantsJSON, respsJSON []byte
	)
	err := pool.QueryRow(ctx, `
		SELECT client_id, client_name, redirect_uris, grant_types, response_types,
		       token_endpoint_auth_method, scope, created_at, updated_at
		FROM oauth_clients
		WHERE client_id = $1
	`, clientID).Scan(
		&c.ClientID, &c.ClientName, &redirectsJSON, &grantsJSON, &respsJSON,
		&c.TokenEndpointAuthMethod, &c.Scope, &c.CreatedAt, &c.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Client{}, ErrClientNotFound
		}
		return Client{}, fmt.Errorf("oauth: get client: %w", err)
	}
	for raw, dst := range map[*[]byte]*[]string{
		&redirectsJSON: &c.RedirectURIs,
		&grantsJSON:    &c.GrantTypes,
		&respsJSON:     &c.ResponseTypes,
	} {
		if len(*raw) > 0 {
			if err := json.Unmarshal(*raw, dst); err != nil {
				return Client{}, fmt.Errorf("oauth: decode client array: %w", err)
			}
		}
	}
	return c, nil
}
