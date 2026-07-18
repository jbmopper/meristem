package auth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
)

var (
	ErrRootExists    = errors.New("auth: root token already exists")
	ErrRootRequired  = errors.New("auth: root token required")
	ErrTokenNotFound = errors.New("auth: token not found")
	ErrTokenShape    = errors.New("auth: token has invalid shape")
	ErrTokenRevoked  = errors.New("auth: token revoked")
	// ErrTokenExpired refuses a bearer whose durable expires_at has passed on
	// the database clock (0037). Expiry is a hard authority cutoff: it must
	// hold no matter which process died, so it is enforced here rather than
	// in any supervisor.
	ErrTokenExpired = errors.New("auth: token expired")
)

// ScopeReviewerCredentialsIssue is the dedicated capability for atomic
// reviewer provisioning (ee916614 slice 3a). It authorizes exactly one
// operation — ProvisionSpawnedReview's in-transaction mint of a single
// exact-child, single-use reviewer credential — plus retiring credentials so
// minted, and is deliberately not implied by any actor class. Deployment
// pins it to the one configured review-dispatch worker identity. The access
// package re-exports it with the other scope constants.
const ScopeReviewerCredentialsIssue = "reviewer_credentials.issue"

// Service owns token creation, revocation, listing, and bearer lookup. All
// state-changing methods append token events; token rows are projections.
type Service struct {
	pool   *pgxpool.Pool
	writer *events.Writer
}

func NewService(pool *pgxpool.Pool, writer *events.Writer) *Service {
	return &Service{pool: pool, writer: writer}
}

type CreateTokenInput struct {
	Name    string
	IsRoot  bool
	Scopes  []string
	Source  domain.Source
	Replace bool
	Actor   *domain.Token
}

type CreateDelegatedTokenInput struct {
	ID     uuid.UUID
	Name   string
	Scopes []string
	Source domain.Source
	Actor  domain.Token
}

type CreateTokenResult struct {
	Token  domain.Token
	Secret string
}

// CreateToken appends token.created, producing the token projection in the
// same transaction. Root bootstrap may run without an actor; non-root tokens
// require an existing root actor.
func (s *Service) CreateToken(ctx context.Context, in CreateTokenInput) (CreateTokenResult, error) {
	if in.Name == "" {
		return CreateTokenResult{}, fmt.Errorf("auth: token name is required")
	}
	if !in.IsRoot {
		if in.Actor == nil || !in.Actor.IsRoot || in.Actor.RevokedAt != nil {
			return CreateTokenResult{}, ErrRootRequired
		}
	}
	eventSource := sourceForToken(in.Actor)

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return CreateTokenResult{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if in.IsRoot {
		var existing uuid.UUID
		err := tx.QueryRow(ctx, `SELECT id FROM tokens WHERE is_root AND revoked_at IS NULL LIMIT 1`).Scan(&existing)
		if err == nil && !in.Replace {
			return CreateTokenResult{}, ErrRootExists
		}
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return CreateTokenResult{}, err
		}
		if err == nil && in.Replace {
			if in.Actor == nil || !in.Actor.IsRoot || in.Actor.RevokedAt != nil {
				return CreateTokenResult{}, ErrRootRequired
			}
			actorID := (*uuid.UUID)(nil)
			actorID = &in.Actor.ID
			if _, _, err := s.writer.Append(ctx, tx, events.Spec{
				SubjectKind:  domain.SubjectToken,
				SubjectID:    existing,
				Kind:         domain.EventTokenRevoked,
				Source:       eventSource,
				ActorTokenID: actorID,
				Payload:      map[string]any{"reason": "replace_root"},
			}); err != nil {
				return CreateTokenResult{}, err
			}
		}
	}

	result, err := s.appendTokenCreated(ctx, tx, appendTokenInput{
		Name:   in.Name,
		IsRoot: in.IsRoot,
		Scopes: in.Scopes,
		Source: in.Source,
		Actor:  in.Actor,
	})
	if err != nil {
		return CreateTokenResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return CreateTokenResult{}, err
	}

	return result, nil
}

// CreateDelegatedToken appends token.created inside a caller-owned
// transaction after a deterministic grant reducer has approved the request.
// It intentionally does not widen CreateToken's root-only semantics.
func (s *Service) CreateDelegatedToken(ctx context.Context, tx pgx.Tx, in CreateDelegatedTokenInput) (CreateTokenResult, error) {
	if in.Actor.ID == uuid.Nil {
		return CreateTokenResult{}, fmt.Errorf("auth: delegated token actor is required")
	}
	if in.Actor.RevokedAt != nil {
		return CreateTokenResult{}, fmt.Errorf("auth: delegated token actor is revoked")
	}
	if in.Actor.IsRoot {
		return CreateTokenResult{}, fmt.Errorf("auth: root token cannot use delegated subactor issuance")
	}
	if in.Actor.Source != domain.SourceAgent {
		return CreateTokenResult{}, fmt.Errorf("auth: delegated token actor must be source=%q", domain.SourceAgent)
	}
	source := in.Source
	if source == "" {
		source = domain.SourceAgent
	}
	if source != domain.SourceAgent {
		return CreateTokenResult{}, fmt.Errorf("auth: delegated token source must be %q", domain.SourceAgent)
	}
	return s.appendTokenCreated(ctx, tx, appendTokenInput{
		Name:   in.Name,
		IsRoot: false,
		Scopes: in.Scopes,
		Source: source,
		Actor:  &in.Actor,
		ID:     in.ID,
	})
}

// MintReviewerCredentialInput describes one single-use reviewer credential
// (ee916614 slice 3a). TemplateScopes is the cultivar's raw ScopesTemplate;
// auth itself resolves every {root} placeholder to ChildID and refuses any
// tree scope that names anything else, so no caller can widen a credential
// past its exact review child. ExpiresAt is required and comes from the
// caller's single database-clock observation so token expiry and the
// binding's ExpiresAt are equal by construction.
type MintReviewerCredentialInput struct {
	Name           string
	ChildID        uuid.UUID
	TemplateScopes []string
	ExpiresAt      time.Time
	Actor          domain.Token
}

// MintReviewerCredential appends token.created inside the caller-owned
// provisioning transaction. The caller must be a non-root system actor
// holding the dedicated reviewer_credentials.issue capability; the minted
// token is always a non-root agent identity with a hard future expiry whose
// tree authority names exactly the one review child. This is the only
// minting path a launcher-side flow may reach.
func (s *Service) MintReviewerCredential(ctx context.Context, tx pgx.Tx, in MintReviewerCredentialInput) (CreateTokenResult, error) {
	if in.Actor.ID == uuid.Nil || in.Actor.IsRoot || in.Actor.RevokedAt != nil {
		return CreateTokenResult{}, fmt.Errorf("auth: reviewer credential minting requires a live non-root actor")
	}
	if in.Actor.Source != domain.SourceSystem {
		return CreateTokenResult{}, fmt.Errorf("auth: reviewer credential minting requires a source=%q actor", domain.SourceSystem)
	}
	if !hasScope(in.Actor.Scopes, ScopeReviewerCredentialsIssue) {
		return CreateTokenResult{}, fmt.Errorf("auth: actor %s lacks the %s capability", in.Actor.ID, ScopeReviewerCredentialsIssue)
	}
	if in.ChildID == uuid.Nil {
		return CreateTokenResult{}, fmt.Errorf("auth: reviewer credential requires the exact review child")
	}
	if in.ExpiresAt.IsZero() {
		return CreateTokenResult{}, fmt.Errorf("auth: reviewer credential requires an expiry")
	}
	var expired bool
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp() >= $1::timestamptz`, in.ExpiresAt).Scan(&expired); err != nil {
		return CreateTokenResult{}, fmt.Errorf("auth: check reviewer credential expiry: %w", err)
	}
	if expired {
		return CreateTokenResult{}, fmt.Errorf("auth: reviewer credential expiry must be in the future")
	}
	if len(in.TemplateScopes) == 0 {
		return CreateTokenResult{}, fmt.Errorf("auth: reviewer credential requires a scopes template")
	}
	// Canonicalize FIRST, then judge (round-2 finding: a blank scope became
	// legacy-unscoped broad authority and a whitespace-prefixed foreign tree
	// slipped the prefix check). Only the closed reviewer-safe vocabulary
	// mints, plus the exact child tree scope, which must be present.
	const treePrefix = "work_items.tree:"
	childTree := treePrefix + in.ChildID.String()
	reviewerSafe := map[string]bool{
		"work_items.read":          true,
		"work_items.write":         true,
		"work_items.tracker_write": true,
		"feed.read_assigned":       true,
	}
	scopes := make([]string, 0, len(in.TemplateScopes))
	seen := make(map[string]bool, len(in.TemplateScopes))
	hasChildTree := false
	for _, raw := range in.TemplateScopes {
		resolved := strings.TrimSpace(strings.ReplaceAll(raw, "{root}", in.ChildID.String()))
		if resolved == "" {
			return CreateTokenResult{}, fmt.Errorf("auth: reviewer credential template contains a blank scope")
		}
		if seen[resolved] {
			return CreateTokenResult{}, fmt.Errorf("auth: reviewer credential template repeats scope %q", resolved)
		}
		seen[resolved] = true
		switch {
		case resolved == childTree:
			hasChildTree = true
		case strings.HasPrefix(resolved, treePrefix):
			return CreateTokenResult{}, fmt.Errorf("auth: reviewer credential tree scope %q does not name the exact review child", raw)
		case reviewerSafe[resolved]:
			// Allowed non-tree vocabulary.
		default:
			return CreateTokenResult{}, fmt.Errorf("auth: scope %q is outside the reviewer-safe vocabulary", resolved)
		}
		scopes = append(scopes, resolved)
	}
	if !hasChildTree {
		return CreateTokenResult{}, fmt.Errorf("auth: reviewer credential must carry the exact child tree scope %s", childTree)
	}
	expiresAt := in.ExpiresAt.UTC()
	return s.appendTokenCreated(ctx, tx, appendTokenInput{
		Name:      in.Name,
		IsRoot:    false,
		Scopes:    scopes,
		Source:    domain.SourceAgent,
		Actor:     &in.Actor,
		ExpiresAt: &expiresAt,
	})
}

// RevokeInTx appends token.revoked inside a caller-owned transaction so a
// launch-failure resolution can revoke the reviewer credential atomically
// with releasing its binding. Actor rules extend Revoke narrowly: root, the
// token revoking itself, or a non-root system actor that holds the
// reviewer_credentials.issue capability — the identity that provisions
// reviewer credentials also retires them, and it can never touch root.
func (s *Service) RevokeInTx(ctx context.Context, tx pgx.Tx, id uuid.UUID, actor domain.Token, reason string) error {
	systemIssuer := !actor.IsRoot && actor.Source == domain.SourceSystem &&
		hasScope(actor.Scopes, ScopeReviewerCredentialsIssue)
	if !actor.IsRoot && actor.ID != id && !systemIssuer {
		return ErrRootRequired
	}
	if reason == "" {
		reason = "operator_revoke"
	}
	tok, err := scanToken(ctx, tx, id)
	if err != nil {
		return err
	}
	if tok.IsRoot && !actor.IsRoot {
		return ErrRootRequired
	}
	if systemIssuer && actor.ID != id && !actor.IsRoot {
		// The issuer retires only the credentials it provisioned: agent
		// tokens linked to a review_launch reservation. Human, system, and
		// unrelated agent tokens are out of its authority entirely.
		if tok.Source != domain.SourceAgent {
			return fmt.Errorf("auth: issuer revocation is limited to reviewer credentials, not source=%q tokens", tok.Source)
		}
		var linked bool
		if err := tx.QueryRow(ctx, `
			SELECT EXISTS (SELECT 1 FROM review_launch WHERE reviewer_token_id = $1)
		`, id).Scan(&linked); err != nil {
			return fmt.Errorf("auth: check reviewer credential linkage: %w", err)
		}
		if !linked {
			return fmt.Errorf("auth: token %s is not a provisioned reviewer credential", id)
		}
	}
	if tok.RevokedAt != nil {
		return nil
	}
	_, _, err = s.writer.Append(ctx, tx, events.Spec{
		SubjectKind:  domain.SubjectToken,
		SubjectID:    id,
		Kind:         domain.EventTokenRevoked,
		Source:       sourceForToken(&actor),
		ActorTokenID: &actor.ID,
		Payload:      map[string]any{"reason": reason},
	})
	return err
}

func hasScope(scopes []string, want string) bool {
	for _, scope := range scopes {
		if scope == want {
			return true
		}
	}
	return false
}

func normalizeTokenSource(in CreateTokenInput) (domain.Source, error) {
	tokenSource := in.Source
	if tokenSource == "" {
		tokenSource = domain.SourceHuman
	}
	if !tokenSource.Valid() {
		return "", fmt.Errorf("auth: invalid token source %q", in.Source)
	}
	if in.IsRoot && tokenSource != domain.SourceHuman {
		return "", fmt.Errorf("auth: root tokens must use source=%q, got %q", domain.SourceHuman, tokenSource)
	}
	return tokenSource, nil
}

type appendTokenInput struct {
	ID        uuid.UUID
	Name      string
	IsRoot    bool
	Scopes    []string
	Source    domain.Source
	Actor     *domain.Token
	ExpiresAt *time.Time
}

func (s *Service) appendTokenCreated(ctx context.Context, tx pgx.Tx, in appendTokenInput) (CreateTokenResult, error) {
	if in.Name == "" {
		return CreateTokenResult{}, fmt.Errorf("auth: token name is required")
	}
	tokenSource, err := normalizeTokenSource(CreateTokenInput{
		IsRoot: in.IsRoot,
		Source: in.Source,
	})
	if err != nil {
		return CreateTokenResult{}, err
	}
	secret, hash, err := NewSecret()
	if err != nil {
		return CreateTokenResult{}, err
	}
	tokenID := in.ID
	if tokenID == uuid.Nil {
		tokenID = uuid.New()
	}
	var actorID *uuid.UUID
	if in.Actor != nil {
		actorID = &in.Actor.ID
	}
	payload := map[string]any{
		"name":    in.Name,
		"hash":    base64.StdEncoding.EncodeToString(hash),
		"is_root": in.IsRoot,
		"scopes":  in.Scopes,
		"source":  tokenSource,
	}
	if in.ExpiresAt != nil {
		payload["expires_at"] = in.ExpiresAt.UTC().Format(time.RFC3339Nano)
	}
	_, _, err = s.writer.Append(ctx, tx, events.Spec{
		SubjectKind:  domain.SubjectToken,
		SubjectID:    tokenID,
		Kind:         domain.EventTokenCreated,
		Source:       sourceForToken(in.Actor),
		ActorTokenID: actorID,
		Payload:      payload,
	})
	if err != nil {
		return CreateTokenResult{}, err
	}
	tok, err := scanToken(ctx, tx, tokenID)
	if err != nil {
		return CreateTokenResult{}, err
	}
	return CreateTokenResult{Token: tok, Secret: secret}, nil
}

func (s *Service) Revoke(ctx context.Context, id uuid.UUID, actor domain.Token) error {
	if !actor.IsRoot && actor.ID != id {
		return ErrRootRequired
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := scanToken(ctx, tx, id); err != nil {
		return err
	}
	_, _, err = s.writer.Append(ctx, tx, events.Spec{
		SubjectKind:  domain.SubjectToken,
		SubjectID:    id,
		Kind:         domain.EventTokenRevoked,
		Source:       sourceForToken(&actor),
		ActorTokenID: &actor.ID,
		Payload:      map[string]any{"reason": "operator_revoke"},
	})
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// RevokeAllNonRoot appends one token.revoked event for each active non-root
// token. The root actor remains active so the owner can recover after panic
// revocation by minting fresh client tokens.
func (s *Service) RevokeAllNonRoot(ctx context.Context, actor domain.Token) ([]uuid.UUID, error) {
	if !actor.IsRoot || actor.RevokedAt != nil {
		return nil, ErrRootRequired
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, `
		SELECT id
		FROM tokens
		WHERE NOT is_root
		  AND revoked_at IS NULL
		ORDER BY created_at, id
	`)
	if err != nil {
		return nil, err
	}
	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()

	for _, id := range ids {
		if _, _, err := s.writer.Append(ctx, tx, events.Spec{
			SubjectKind:  domain.SubjectToken,
			SubjectID:    id,
			Kind:         domain.EventTokenRevoked,
			Source:       sourceForToken(&actor),
			ActorTokenID: &actor.ID,
			Payload:      map[string]any{"reason": "panic_revoke"},
		}); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return ids, nil
}

func (s *Service) Authenticate(ctx context.Context, secret string) (domain.Token, error) {
	if !ValidSecretShape(secret) {
		return domain.Token{}, ErrTokenShape
	}
	hash := HashSecret(secret)
	// Expiry is judged by the database clock in the same read: a process
	// whose local clock lags must not extend a credential's authority.
	var expired bool
	tok, err := scanTokenRowExtra(s.pool.QueryRow(ctx, `
		SELECT id, name, hash, is_root, scopes, source, created_at, revoked_at, expires_at,
		       (expires_at IS NOT NULL AND expires_at <= now())
		FROM tokens
		WHERE hash = $1
	`, hash), &expired)
	if err != nil {
		if errors.Is(err, ErrTokenNotFound) {
			return domain.Token{}, ErrInvalidToken
		}
		return domain.Token{}, err
	}
	if !EqualHash(tok.Hash, hash) {
		return domain.Token{}, ErrInvalidToken
	}
	if tok.RevokedAt != nil {
		return domain.Token{}, ErrTokenRevoked
	}
	if expired {
		return domain.Token{}, ErrTokenExpired
	}
	return tok, nil
}

func (s *Service) Get(ctx context.Context, id uuid.UUID) (domain.Token, error) {
	return scanToken(ctx, s.pool, id)
}

// ValidateLive re-reads a token by id and refuses revoked or expired ones
// against the database clock. Long-lived sessions (the stdio MCP server)
// call this on every protected dispatch so a cached authentication can
// never outlive the token's durable authority (ee916614 slice 3a round-1
// finding: expiry and revocation must bite mid-session, not only at the
// initial handshake).
func (s *Service) ValidateLive(ctx context.Context, id uuid.UUID) (domain.Token, error) {
	if id == uuid.Nil {
		return domain.Token{}, ErrInvalidToken
	}
	var expired bool
	tok, err := scanTokenRowExtra(s.pool.QueryRow(ctx, `
		SELECT id, name, hash, is_root, scopes, source, created_at, revoked_at, expires_at,
		       (expires_at IS NOT NULL AND expires_at <= now())
		FROM tokens
		WHERE id = $1
	`, id), &expired)
	if err != nil {
		return domain.Token{}, err
	}
	if tok.RevokedAt != nil {
		return domain.Token{}, ErrTokenRevoked
	}
	if expired {
		return domain.Token{}, ErrTokenExpired
	}
	return tok, nil
}

func (s *Service) List(ctx context.Context) ([]domain.Token, error) {
	rows, err := s.pool.Query(ctx, `SELECT id, name, hash, is_root, scopes, source, created_at, revoked_at, expires_at FROM tokens ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Token
	for rows.Next() {
		tok, err := scanTokenRow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, tok)
	}
	return out, rows.Err()
}

type rowScanner interface {
	Scan(dest ...any) error
}

type queryer interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func scanToken(ctx context.Context, q queryer, id uuid.UUID) (domain.Token, error) {
	row := q.QueryRow(ctx, `SELECT id, name, hash, is_root, scopes, source, created_at, revoked_at, expires_at FROM tokens WHERE id = $1`, id)
	return scanTokenRow(row)
}

func scanTokenRow(row rowScanner) (domain.Token, error) {
	return scanTokenRowExtra(row)
}

// scanTokenRowExtra scans the canonical token columns plus any caller-added
// trailing expressions (Authenticate's database-clock expiry check).
func scanTokenRowExtra(row rowScanner, extra ...any) (domain.Token, error) {
	var tok domain.Token
	var scopesRaw []byte
	var source string
	var revokedAt *time.Time
	dest := []any{&tok.ID, &tok.Name, &tok.Hash, &tok.IsRoot, &scopesRaw, &source, &tok.CreatedAt, &revokedAt, &tok.ExpiresAt}
	dest = append(dest, extra...)
	if err := row.Scan(dest...); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Token{}, ErrTokenNotFound
		}
		return domain.Token{}, err
	}
	if len(scopesRaw) > 0 {
		if err := json.Unmarshal(scopesRaw, &tok.Scopes); err != nil {
			return domain.Token{}, err
		}
	}
	tok.Source = domain.Source(source)
	if !tok.Source.Valid() {
		tok.Source = domain.SourceHuman
	}
	tok.RevokedAt = revokedAt
	return tok, nil
}

func sourceForToken(tok *domain.Token) domain.Source {
	if tok != nil && tok.Source.Valid() {
		return tok.Source
	}
	return domain.SourceHuman
}
