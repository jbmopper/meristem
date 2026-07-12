package oauth

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
)

// ErrInvalidRegistration is returned when a dynamic client registration
// request is malformed or unsafe (RFC 7591 §3.2.2 invalid_client_metadata /
// invalid_redirect_uri). Callers map it to an HTTP 400.
var ErrInvalidRegistration = errors.New("oauth: invalid client registration")

// RegistrationService records dynamic client registrations as
// oauth_client.registered events. It issues no client secret: provider clients
// are public and authenticate with PKCE.
type RegistrationService struct {
	pool          *pgxpool.Pool
	writer        *events.Writer
	systemActorID uuid.UUID
}

func NewRegistrationService(pool *pgxpool.Pool, writer *events.Writer) *RegistrationService {
	return &RegistrationService{pool: pool, writer: writer}
}

func NewRegistrationServiceWithSystemActor(pool *pgxpool.Pool, writer *events.Writer, actorID uuid.UUID) *RegistrationService {
	return &RegistrationService{pool: pool, writer: writer, systemActorID: actorID}
}

// RegisterInput is the subset of RFC 7591 client metadata the gateway accepts.
// Omitted grant/response types default to authorization_code / code. Only the
// public-client auth method ("none", or empty) is accepted.
type RegisterInput struct {
	ClientName              string
	RedirectURIs            []string
	TokenEndpointAuthMethod string
	Scope                   string
}

// RegisteredClient is the registration response (RFC 7591 §3.2.1), all
// non-secret. client_id_issued_at is the event occurred_at at the projection.
type RegisteredClient struct {
	ClientID                string
	ClientName              string
	RedirectURIs            []string
	GrantTypes              []string
	ResponseTypes           []string
	TokenEndpointAuthMethod string
	Scope                   string
}

// Register validates the requested client metadata, generates a non-secret
// client_id, and appends one oauth_client.registered event whose projection is
// the oauth_clients row the authorize endpoint later reads.
func (s *RegistrationService) Register(ctx context.Context, in RegisterInput) (RegisteredClient, error) {
	if s.pool == nil || s.writer == nil {
		return RegisteredClient{}, errors.New("oauth: registration service is not configured")
	}
	if len(in.ClientName) > MaxClientNameLength || len(in.Scope) > MaxScopeLength {
		return RegisteredClient{}, fmt.Errorf("%w: client_name or scope exceeds resource limit", ErrInvalidRegistration)
	}
	authMethod := strings.TrimSpace(in.TokenEndpointAuthMethod)
	if authMethod == "" {
		authMethod = AuthMethodNone
	}
	if authMethod != AuthMethodNone {
		return RegisteredClient{}, fmt.Errorf("%w: token_endpoint_auth_method %q is unsupported; provider clients must be public (none) and use PKCE", ErrInvalidRegistration, authMethod)
	}
	scope, err := normalizeRegistrationScope(in.Scope)
	if err != nil {
		return RegisteredClient{}, fmt.Errorf("%w: %v", ErrInvalidRegistration, err)
	}

	redirectURIs, err := validateRedirectURIs(in.RedirectURIs)
	if err != nil {
		return RegisteredClient{}, err
	}

	clientID, err := generateClientID()
	if err != nil {
		return RegisteredClient{}, err
	}
	systemActor, err := loadActor(ctx, s.pool, s.systemActorID, domain.SourceSystem)
	if err != nil {
		return RegisteredClient{}, err
	}

	payload := registeredPayload{
		PayloadVersion:          1,
		ClientID:                clientID,
		ClientName:              strings.TrimSpace(in.ClientName),
		RedirectURIs:            redirectURIs,
		GrantTypes:              []string{GrantAuthorizationCode},
		ResponseTypes:           []string{ResponseTypeCode},
		TokenEndpointAuthMethod: AuthMethodNone,
		Scope:                   scope,
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return RegisteredClient{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, _, err := s.writer.Append(ctx, tx, events.Spec{
		SubjectKind:  domain.SubjectOAuthClient,
		SubjectID:    ClientSubjectID(clientID),
		Kind:         domain.EventOAuthClientRegistered,
		Source:       domain.SourceSystem,
		ActorTokenID: &systemActor.ID,
		Payload:      payload,
	}); err != nil {
		return RegisteredClient{}, fmt.Errorf("oauth: append oauth_client.registered: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return RegisteredClient{}, err
	}

	return RegisteredClient{
		ClientID:                clientID,
		ClientName:              payload.ClientName,
		RedirectURIs:            redirectURIs,
		GrantTypes:              payload.GrantTypes,
		ResponseTypes:           payload.ResponseTypes,
		TokenEndpointAuthMethod: AuthMethodNone,
		Scope:                   payload.Scope,
	}, nil
}
