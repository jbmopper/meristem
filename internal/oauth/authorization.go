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

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/approvals"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/idempotency"
	"github.com/jbmopper/meristem/internal/workitems"
)

const AuthorizationRequestTTL = 10 * time.Minute

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
	return NewAuthorizationServiceWithClock(pool, writer, wi, ap, systemActorID, nil)
}

func NewAuthorizationServiceWithClock(pool *pgxpool.Pool, writer *events.Writer, wi *workitems.Service, ap *approvals.Service, systemActorID uuid.UUID, clock func() time.Time) *AuthorizationService {
	if clock == nil {
		clock = time.Now
	}
	return &AuthorizationService{pool: pool, writer: writer, workItems: wi, approvals: ap, systemActorID: systemActorID, codes: NewAuthCodeService(pool, writer), now: clock}
}

type AuthorizationInput struct{ ClientID, RedirectURI, ResponseType, State, CodeChallenge, CodeChallengeMethod, Scope, Resource, ExpectedResource string }
type AuthorizationRequest struct {
	ID, WorkItemID, ApprovalID                    uuid.UUID
	ClientID, RedirectURI, State, Scope, Resource string
	ExpiresAt                                     time.Time
}

func (s *AuthorizationService) Begin(ctx context.Context, in AuthorizationInput) (AuthorizationRequest, error) {
	if len(in.ClientID) > 256 || len(in.RedirectURI) > MaxRedirectURILength || len(in.State) > 4096 || len(in.CodeChallenge) > 256 || len(in.Scope) > MaxScopeLength || len(in.Resource) > MaxRedirectURILength {
		return AuthorizationRequest{}, fmt.Errorf("%w: field exceeds resource limit", ErrInvalidAuthorizationRequest)
	}
	systemActor, err := loadActor(ctx, s.pool, s.systemActorID, domain.SourceSystem)
	if err != nil {
		return AuthorizationRequest{}, err
	}
	client, err := GetClient(ctx, s.pool, strings.TrimSpace(in.ClientID))
	if err != nil {
		if errors.Is(err, ErrClientNotFound) {
			return AuthorizationRequest{}, fmt.Errorf("%w: unknown client_id", ErrInvalidAuthorizationRequest)
		}
		return AuthorizationRequest{}, err
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
	if err := ValidateCodeChallenge(in.CodeChallenge, in.CodeChallengeMethod); err != nil {
		return AuthorizationRequest{}, fmt.Errorf("%w: %v", ErrInvalidAuthorizationRequest, err)
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
	sealedProfile, err := access.ProviderAuthorityProfileFromScopes(providerActor.Scopes)
	if err != nil || string(sealedProfile) != client.AuthorityProfile {
		return AuthorizationRequest{}, ErrProviderActorUnavailable
	}
	expectedScope, err := OAuthScopeForAuthorityProfile(sealedProfile)
	if err != nil || !registrationScopeAllows(client.Scope, expectedScope) {
		return AuthorizationRequest{}, ErrProviderActorUnavailable
	}
	scope, err := normalizeScopeForProfile(in.Scope, sealedProfile)
	if err != nil {
		return AuthorizationRequest{}, err
	}
	identity := strings.Join([]string{in.ClientID, in.RedirectURI, in.ResponseType, in.State, in.CodeChallenge, in.CodeChallengeMethod, scope, in.Resource}, "\x00")
	hash := sha256.Sum256([]byte(identity))
	ctx = idempotency.WithRequest(ctx, idempotency.Request{TokenID: systemActor.ID, Scope: "oauth.authorize", Key: hex.EncodeToString(hash[:]), RequestHash: hash[:]})
	reqID, ok := idempotency.SubjectID(ctx, "oauth_authorization_request")
	if !ok {
		return AuthorizationRequest{}, errors.New("oauth: deterministic authorization identity unavailable")
	}
	item, err := s.workItems.Create(ctx, workitems.CreateInput{Title: "OAuth access request: " + client.ClientID, Body: fmt.Sprintf("UNTRUSTED self-asserted client name: %q. Selected redirect: %s. Registered redirects: %s. Bound actor: %s. Sealed profile: %s. Effective scopes: %s. Resource: %s.", client.ClientName, in.RedirectURI, strings.Join(client.RedirectURIs, ", "), providerActor.ID, client.AuthorityProfile, strings.Join(providerActor.Scopes, ", "), in.Resource), Actor: systemActor, State: domain.WorkItemCaptured, HumanReviewStatus: domain.HumanReviewBlocked, PatienceBudgetSeconds: int(AuthorizationRequestTTL.Seconds()), EscalationRule: domain.EscalationRuleHandToHuman})
	if err != nil {
		return AuthorizationRequest{}, err
	}
	if existing, found, err := s.getExistingRequest(ctx, reqID); err != nil {
		return AuthorizationRequest{}, err
	} else if found {
		return existing, nil
	}
	expires := item.CreatedAt.UTC().Add(AuthorizationRequestTTL).Truncate(time.Microsecond)
	approvalResult, err := s.approvals.Create(ctx, approvals.CreateInput{WorkItemID: item.ID, Summary: "Authorize OAuth client " + client.ClientID + " with " + client.AuthorityProfile, Request: map[string]any{"client_id": client.ClientID, "client_name_untrusted": client.ClientName, "redirect_uri": in.RedirectURI, "registered_redirect_uris": client.RedirectURIs, "actor_token_id": providerActor.ID, "authority_profile": client.AuthorityProfile, "effective_scopes": providerActor.Scopes, "oauth_scope": scope, "resource": in.Resource}, ExpiresAt: expires, Actor: systemActor})
	if err != nil {
		return AuthorizationRequest{}, err
	}
	approvalID := approvalResult.Approval.ID
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return AuthorizationRequest{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	_, _, err = s.writer.Append(ctx, tx, events.Spec{SubjectKind: domain.SubjectOAuthAuthorizationRequest, SubjectID: reqID, Kind: domain.EventOAuthAuthorizationRequestCreated, Source: domain.SourceSystem, ActorTokenID: &systemActor.ID, Payload: map[string]any{"payload_version": 1, "authorization_request_id": reqID, "work_item_id": item.ID, "approval_id": approvalID, "client_id": client.ClientID, "redirect_uri": in.RedirectURI, "response_type": in.ResponseType, "state": in.State, "code_challenge": in.CodeChallenge, "code_challenge_method": in.CodeChallengeMethod, "scope": scope, "resource": in.Resource, "actor_token_id": providerActor.ID, "authority_profile": client.AuthorityProfile, "expires_at_unix": expires.Unix()}})
	if err != nil {
		return AuthorizationRequest{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return AuthorizationRequest{}, err
	}
	return AuthorizationRequest{ID: reqID, WorkItemID: item.ID, ApprovalID: approvalID, ClientID: client.ClientID, RedirectURI: in.RedirectURI, State: in.State, Scope: scope, Resource: in.Resource, ExpiresAt: expires}, nil
}

func (s *AuthorizationService) getExistingRequest(ctx context.Context, id uuid.UUID) (AuthorizationRequest, bool, error) {
	var out AuthorizationRequest
	out.ID = id
	err := s.pool.QueryRow(ctx, `SELECT work_item_id,approval_id,client_id,redirect_uri,state,scope,resource,expires_at FROM oauth_authorization_requests WHERE id=$1`, id).Scan(&out.WorkItemID, &out.ApprovalID, &out.ClientID, &out.RedirectURI, &out.State, &out.Scope, &out.Resource, &out.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return AuthorizationRequest{}, false, nil
	}
	if err != nil {
		return AuthorizationRequest{}, false, err
	}
	return out, true, nil
}

func (s *AuthorizationService) ensureBindingWorkItem(ctx context.Context, client Client, systemActor domain.Token) (uuid.UUID, error) {
	if client.BindingWorkItemID != nil {
		return *client.BindingWorkItemID, nil
	}
	hash := sha256.Sum256([]byte(client.ClientID))
	ctx = idempotency.WithRequest(ctx, idempotency.Request{TokenID: systemActor.ID, Scope: "oauth.bind_provider_actor", Key: client.ClientID, RequestHash: hash[:]})
	item, err := s.workItems.Create(ctx, workitems.CreateInput{Title: "Bind OAuth provider actor: " + client.ClientID, Body: fmt.Sprintf("Scoped human administration required (oauth_clients.bind). Client name %q is UNTRUSTED self-asserted metadata. Registered redirect URIs: %s. Bind this client to a pre-provisioned source=agent token whose exact scopes match one sealed authority profile, then retry authorization.", client.ClientName, strings.Join(client.RedirectURIs, ", ")), Actor: systemActor, State: domain.WorkItemCaptured, HumanReviewStatus: domain.HumanReviewBlocked, PatienceBudgetSeconds: 86400, EscalationRule: domain.EscalationRuleHandToHuman})
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
	if errors.Is(err, pgx.ErrNoRows) {
		return ContinuationResult{}, fmt.Errorf("%w: authorization request not found", ErrInvalidAuthorizationRequest)
	}
	if err != nil {
		return ContinuationResult{}, err
	}
	if req.completed != nil {
		return ContinuationResult{}, fmt.Errorf("%w: authorization request already completed", ErrInvalidAuthorizationRequest)
	}
	var status string
	err = tx.QueryRow(ctx, `SELECT status FROM approvals WHERE id=$1 FOR UPDATE`, req.approvalID).Scan(&status)
	if err != nil {
		return ContinuationResult{}, err
	}
	now := s.now().UTC()
	if status == string(approvals.StatusPending) && now.Before(req.expires) {
		return ContinuationResult{Pending: true, WorkItemID: req.workItemID}, nil
	}
	outcome := status
	oauthErr := ""
	code := ""
	desiredWorkItemState := domain.WorkItemFailed
	if status == string(approvals.StatusApproved) && now.Before(req.expires) {
		code, err = s.codes.issueInTx(ctx, tx, IssueInput{ClientID: req.clientID, RedirectURI: req.redirectURI, CodeChallenge: req.challenge, CodeChallengeMethod: req.method, Scope: req.scope, Resource: req.resource, ActorTokenID: req.actorID, SystemActorTokenID: systemActor.ID, AuthorityProfile: req.authorityProfile})
		if err != nil {
			return ContinuationResult{}, err
		}
		outcome = "approved"
		desiredWorkItemState = domain.WorkItemDone
	} else if status == string(approvals.StatusDenied) {
		oauthErr = "access_denied"
		outcome = "denied"
	} else {
		oauthErr = "temporarily_unavailable"
		outcome = "expired"
		if status == string(approvals.StatusPending) {
			_, _, err = s.writer.Append(ctx, tx, events.Spec{
				SubjectKind: domain.SubjectApproval, SubjectID: req.approvalID,
				Kind: domain.EventApprovalExpired, Source: domain.SourceSystem, ActorTokenID: &systemActor.ID,
				Discriminator: id.String(),
				Payload:       map[string]any{"work_item_id": req.workItemID, "reason": "oauth_authorization_expired"},
			})
			if err != nil {
				return ContinuationResult{}, err
			}
		}
	}
	_, _, err = s.writer.Append(ctx, tx, events.Spec{SubjectKind: domain.SubjectOAuthAuthorizationRequest, SubjectID: id, Kind: domain.EventOAuthAuthorizationRequestCompleted, Source: domain.SourceSystem, ActorTokenID: &systemActor.ID, Payload: map[string]any{"payload_version": 1, "authorization_request_id": id, "outcome": outcome, "completed_at_unix": now.Unix()}})
	if err != nil {
		return ContinuationResult{}, err
	}
	if err := s.appendWorkItemTransitionInTx(ctx, tx, req.workItemID, desiredWorkItemState, "oauth_authorization_"+outcome, systemActor, id.String()); err != nil {
		return ContinuationResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ContinuationResult{}, err
	}
	return ContinuationResult{RedirectURI: req.redirectURI, State: req.state, Code: code, OAuthError: oauthErr, WorkItemID: req.workItemID}, nil
}

func (s *AuthorizationService) appendWorkItemTransitionInTx(ctx context.Context, tx pgx.Tx, id uuid.UUID, to domain.WorkItemState, reason string, actor domain.Token, discriminator string) error {
	var from domain.WorkItemState
	if err := tx.QueryRow(ctx, `SELECT state FROM work_items WHERE id=$1 FOR UPDATE`, id).Scan(&from); err != nil {
		return err
	}
	if from == to {
		return nil
	}
	if !domain.CanTransition(from, to) {
		return fmt.Errorf("oauth: invalid authorization work item transition from %s to %s", from, to)
	}
	_, _, err := s.writer.Append(ctx, tx, events.Spec{
		SubjectKind: domain.SubjectWorkItem, SubjectID: id,
		Kind: domain.EventWorkItemTransitioned, Source: actor.Source, ActorTokenID: &actor.ID,
		Discriminator: discriminator,
		Payload:       map[string]any{"from": from, "to": to, "reason": reason},
	})
	return err
}
