package oauth

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/approvals"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/idempotency"
	"github.com/jbmopper/meristem/internal/workitems"
)

const AuthorizationRequestTTL = 10 * time.Minute
const ScopeMCPRead = "mcp:read"

var ErrInvalidAuthorizationRequest = errors.New("oauth: invalid authorization request")

type AuthorizationService struct {
	pool          *pgxpool.Pool
	writer        *events.Writer
	workItems     *workitems.Service
	approvals     *approvals.Service
	systemActorID uuid.UUID
	codes         *AuthCodeService
	now           func() time.Time
}

func NewAuthorizationService(pool *pgxpool.Pool, writer *events.Writer, wi *workitems.Service, ap *approvals.Service, systemActorID uuid.UUID) *AuthorizationService {
	return &AuthorizationService{pool: pool, writer: writer, workItems: wi, approvals: ap, systemActorID: systemActorID, codes: NewAuthCodeService(pool, writer), now: time.Now}
}

type AuthorizationInput struct{ ClientID, RedirectURI, ResponseType, State, CodeChallenge, CodeChallengeMethod, Scope, Resource, ExpectedResource string }
type AuthorizationRequest struct {
	ID, WorkItemID, ApprovalID                    uuid.UUID
	ClientID, RedirectURI, State, Scope, Resource string
	ExpiresAt                                     time.Time
}

func (s *AuthorizationService) Begin(ctx context.Context, in AuthorizationInput) (AuthorizationRequest, error) {
	systemActor, err := loadActor(ctx, s.pool, s.systemActorID, domain.SourceSystem)
	if err != nil {
		return AuthorizationRequest{}, err
	}
	client, err := GetClient(ctx, s.pool, strings.TrimSpace(in.ClientID))
	if err != nil {
		return AuthorizationRequest{}, fmt.Errorf("%w: unknown client_id", ErrInvalidAuthorizationRequest)
	}
	if client.RevokedAt != nil {
		return AuthorizationRequest{}, fmt.Errorf("%w: client revoked", ErrInvalidAuthorizationRequest)
	}
	if !client.AllowsRedirectURI(in.RedirectURI) {
		return AuthorizationRequest{}, fmt.Errorf("%w: redirect_uri is not registered", ErrInvalidAuthorizationRequest)
	}
	if in.ResponseType != ResponseTypeCode {
		return AuthorizationRequest{}, fmt.Errorf("%w: response_type must be code", ErrInvalidAuthorizationRequest)
	}
	if in.Resource == "" || in.Resource != in.ExpectedResource {
		return AuthorizationRequest{}, fmt.Errorf("%w: resource must exactly match %s", ErrInvalidAuthorizationRequest, in.ExpectedResource)
	}
	scope, err := normalizeScope(in.Scope)
	if err != nil {
		return AuthorizationRequest{}, err
	}
	if err := ValidateCodeChallenge(in.CodeChallenge, in.CodeChallengeMethod); err != nil {
		return AuthorizationRequest{}, err
	}
	if client.ActorTokenID == nil {
		bindingID, bindErr := s.ensureBindingWorkItem(ctx, client, systemActor)
		if bindErr != nil {
			return AuthorizationRequest{}, bindErr
		}
		return AuthorizationRequest{}, fmt.Errorf("%w: bind client %s to a pre-provisioned source=agent token using work item %s, then retry authorization", ErrProviderActorUnavailable, client.ClientID, bindingID)
	}
	providerActor, err := validateProviderActor(ctx, s.pool, *client.ActorTokenID)
	if err != nil {
		return AuthorizationRequest{}, err
	}
	identity := strings.Join([]string{in.ClientID, in.RedirectURI, in.ResponseType, in.State, in.CodeChallenge, in.CodeChallengeMethod, scope, in.Resource}, "\x00")
	hash := sha256.Sum256([]byte(identity))
	ctx = idempotency.WithRequest(ctx, idempotency.Request{TokenID: systemActor.ID, Scope: "oauth.authorize", Key: hex.EncodeToString(hash[:]), RequestHash: hash[:]})
	expires := s.now().UTC().Add(AuthorizationRequestTTL)
	item, err := s.workItems.Create(ctx, workitems.CreateInput{Title: "OAuth access request: " + client.ClientName, Body: "Provider OAuth client requests authority profile " + client.AuthorityProfile + " for the Meristem MCP resource.", Actor: systemActor, State: domain.WorkItemCaptured, HumanReviewStatus: domain.HumanReviewBlocked, PatienceBudgetSeconds: int(AuthorizationRequestTTL.Seconds()), EscalationRule: domain.EscalationRuleHandToHuman})
	if err != nil {
		return AuthorizationRequest{}, err
	}
	approvalResult, err := s.approvals.Create(ctx, approvals.CreateInput{WorkItemID: item.ID, Summary: "Authorize provider MCP access: " + client.AuthorityProfile, Request: map[string]any{"client_id": client.ClientID, "client_name": client.ClientName, "authority_profile": client.AuthorityProfile, "scope": scope, "resource": in.Resource}, ExpiresIn: AuthorizationRequestTTL, Actor: systemActor})
	if err != nil {
		return AuthorizationRequest{}, err
	}
	approvalID := approvalResult.Approval.ID
	reqID, ok := idempotency.SubjectID(ctx, "oauth_authorization_request")
	if !ok {
		return AuthorizationRequest{}, errors.New("oauth: deterministic authorization identity unavailable")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return AuthorizationRequest{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	_, _, err = s.writer.Append(ctx, tx, events.Spec{SubjectKind: domain.SubjectOAuthAuthorizationRequest, SubjectID: reqID, Kind: domain.EventOAuthAuthorizationRequestCreated, Source: domain.SourceSystem, ActorTokenID: &systemActor.ID, Payload: map[string]any{"payload_version": 1, "work_item_id": item.ID, "approval_id": approvalID, "client_id": client.ClientID, "redirect_uri": in.RedirectURI, "response_type": in.ResponseType, "state": in.State, "code_challenge": in.CodeChallenge, "code_challenge_method": in.CodeChallengeMethod, "scope": scope, "resource": in.Resource, "actor_token_id": providerActor.ID, "authority_profile": client.AuthorityProfile, "expires_at_unix": expires.Unix()}})
	if err != nil {
		return AuthorizationRequest{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return AuthorizationRequest{}, err
	}
	return AuthorizationRequest{ID: reqID, WorkItemID: item.ID, ApprovalID: approvalID, ClientID: client.ClientID, RedirectURI: in.RedirectURI, State: in.State, Scope: scope, Resource: in.Resource, ExpiresAt: expires}, nil
}

func (s *AuthorizationService) ensureBindingWorkItem(ctx context.Context, client Client, systemActor domain.Token) (uuid.UUID, error) {
	if client.BindingWorkItemID != nil {
		return *client.BindingWorkItemID, nil
	}
	hash := sha256.Sum256([]byte(client.ClientID))
	ctx = idempotency.WithRequest(ctx, idempotency.Request{TokenID: systemActor.ID, Scope: "oauth.bind_provider_actor", Key: client.ClientID, RequestHash: hash[:]})
	item, err := s.workItems.Create(ctx, workitems.CreateInput{Title: "Bind OAuth provider actor: " + client.ClientID, Body: "Root action required: bind this registered OAuth client to a pre-provisioned source=agent token and an approved authority profile, then retry authorization.", Actor: systemActor, State: domain.WorkItemCaptured, HumanReviewStatus: domain.HumanReviewBlocked, PatienceBudgetSeconds: 86400, EscalationRule: domain.EscalationRuleHandToHuman})
	if err != nil {
		return uuid.Nil, err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return uuid.Nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	_, _, err = s.writer.Append(ctx, tx, events.Spec{SubjectKind: domain.SubjectOAuthClient, SubjectID: ClientSubjectID(client.ClientID), Kind: domain.EventOAuthClientActorBindingRequested, Source: domain.SourceSystem, ActorTokenID: &systemActor.ID, Payload: map[string]any{"payload_version": 1, "client_id": client.ClientID, "work_item_id": item.ID}})
	if err != nil {
		return uuid.Nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return uuid.Nil, err
	}
	return item.ID, nil
}

func normalizeScope(raw string) (string, error) {
	fields := strings.Fields(raw)
	if len(fields) == 0 {
		return ScopeMCPRead, nil
	}
	if len(fields) != 1 || fields[0] != ScopeMCPRead {
		return "", fmt.Errorf("%w: only scope %s is supported", ErrInvalidAuthorizationRequest, ScopeMCPRead)
	}
	return ScopeMCPRead, nil
}

type ContinuationResult struct {
	Pending     bool
	RedirectURI string
	State       string
	Code        string
	OAuthError  string
	WorkItemID  uuid.UUID
}

func (s *AuthorizationService) Continue(ctx context.Context, id uuid.UUID) (ContinuationResult, error) {
	systemActor, err := loadActor(ctx, s.pool, s.systemActorID, domain.SourceSystem)
	if err != nil {
		return ContinuationResult{}, err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return ContinuationResult{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var req struct {
		workItemID, approvalID, actorID                                  uuid.UUID
		clientID, redirectURI, state, challenge, method, scope, resource string
		authorityProfile                                                 string
		expires                                                          time.Time
		completed                                                        *time.Time
	}
	err = tx.QueryRow(ctx, `SELECT work_item_id,approval_id,client_id,redirect_uri,state,code_challenge,code_challenge_method,scope,resource,actor_token_id,authority_profile,expires_at,completed_at FROM oauth_authorization_requests WHERE id=$1 FOR UPDATE`, id).Scan(&req.workItemID, &req.approvalID, &req.clientID, &req.redirectURI, &req.state, &req.challenge, &req.method, &req.scope, &req.resource, &req.actorID, &req.authorityProfile, &req.expires, &req.completed)
	if err != nil {
		return ContinuationResult{}, err
	}
	if req.completed != nil {
		return ContinuationResult{}, fmt.Errorf("%w: authorization request already completed", ErrInvalidAuthorizationRequest)
	}
	var status string
	err = tx.QueryRow(ctx, `SELECT status FROM approvals WHERE id=$1`, req.approvalID).Scan(&status)
	if err != nil {
		return ContinuationResult{}, err
	}
	if status == string(approvals.StatusPending) && s.now().UTC().Before(req.expires) {
		return ContinuationResult{Pending: true, WorkItemID: req.workItemID}, nil
	}
	outcome := status
	oauthErr := ""
	code := ""
	if status == string(approvals.StatusApproved) {
		code, err = s.codes.issueInTx(ctx, tx, IssueInput{ClientID: req.clientID, RedirectURI: req.redirectURI, CodeChallenge: req.challenge, CodeChallengeMethod: req.method, Scope: req.scope, Resource: req.resource, ActorTokenID: req.actorID, SystemActorTokenID: systemActor.ID, AuthorityProfile: req.authorityProfile})
		if err != nil {
			return ContinuationResult{}, err
		}
		outcome = "approved"
	} else if status == string(approvals.StatusDenied) {
		oauthErr = "access_denied"
		outcome = "denied"
	} else {
		oauthErr = "temporarily_unavailable"
		outcome = "expired"
	}
	_, _, err = s.writer.Append(ctx, tx, events.Spec{SubjectKind: domain.SubjectOAuthAuthorizationRequest, SubjectID: id, Kind: domain.EventOAuthAuthorizationRequestCompleted, Source: domain.SourceSystem, ActorTokenID: &systemActor.ID, Payload: map[string]any{"payload_version": 1, "outcome": outcome, "completed_at_unix": s.now().UTC().Unix()}})
	if err != nil {
		return ContinuationResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ContinuationResult{}, err
	}
	if outcome == "approved" {
		_, _ = s.workItems.Transition(ctx, req.workItemID, domain.WorkItemDone, "oauth authorization completed", systemActor)
	}
	return ContinuationResult{RedirectURI: req.redirectURI, State: req.state, Code: code, OAuthError: oauthErr, WorkItemID: req.workItemID}, nil
}
