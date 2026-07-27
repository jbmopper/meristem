package idempotency

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/safety"
)

const headerName = "Idempotency-Key"
const ttl = 24 * time.Hour

// unlockTimeout bounds the best-effort pg_advisory_unlock so a hung
// connection cannot stall request teardown indefinitely. Session-level
// advisory locks held on a connection that gets force-closed are
// released by Postgres on backend exit, so even if unlock times out
// we are not leaking a lock permanently — we are only waiting longer
// for it to drop.
const unlockTimeout = 5 * time.Second

// Backoff schedule for pg_try_advisory_lock retries. Waiters do NOT
// hold a pool connection between tries — that's the whole reason for
// try_lock instead of advisory_lock: a busy lock with N concurrent
// waiters must not block the pool conn the lock holder's handler
// needs to do its own work. The schedule is exponential with a soft
// cap so a long handler doesn't spin tight.
const (
	lockTryInitialBackoff = 1 * time.Millisecond
	lockTryMaxBackoff     = 50 * time.Millisecond
)

// Middleware replays successful POST responses for the same
// (token, method+path, key, body hash). It stores its cache through an
// idempotency.recorded event so replay state is rebuildable from events.
type Middleware struct {
	pool   *pgxpool.Pool
	writer *events.Writer
	guard  func() error
}

func NewMiddleware(pool *pgxpool.Pool, writer *events.Writer) *Middleware {
	return &Middleware{pool: pool, writer: writer}
}

// NewMiddlewareWithGuard installs a dynamic runtime guard around every
// handler, cache-replay, and response boundary. The check is deliberately
// re-run after advisory-lock waits and immediately before a response so a
// process that becomes ineligible while a request is in flight fails closed.
func NewMiddlewareWithGuard(pool *pgxpool.Pool, writer *events.Writer, guard func() error) *Middleware {
	return &Middleware{pool: pool, writer: writer, guard: guard}
}

// ExecuteInput describes one non-HTTP mutation guarded by the same durable
// idempotency store as POST middleware. Run must return a JSON response body
// for successful execution. Semantic tool/API refusals should be encoded in
// that body with a 4xx status and a nil error; a refusal that provably
// committed nothing may additionally call MarkRefusalUnconsumed on the run
// context to leave the key usable by a corrected retry, while unmarked
// refusals are recorded and replayed like committed conclusions.
type ExecuteInput struct {
	Token       domain.Token
	Scope       string
	Key         string
	RequestHash []byte
	Run         func(context.Context) (status int, body []byte, err error)
}

// ExecuteResult is the canonical recorded response for an idempotent mutation.
type ExecuteResult struct {
	Status   int
	Body     []byte
	Replayed bool
}

// Execute runs a non-HTTP mutation under the same durable idempotency contract
// as Wrap: fast cache lookup, advisory-lock serialization, same-key /
// different-body conflict, context injection for stable subject ids, and
// idempotency.recorded persistence.
func (m *Middleware) Execute(ctx context.Context, in ExecuteInput) (ExecuteResult, error) {
	if m == nil || m.pool == nil || m.writer == nil {
		return ExecuteResult{}, fmt.Errorf("idempotency executor not configured")
	}
	if in.Token.ID == uuid.Nil {
		return ExecuteResult{}, fmt.Errorf("idempotency token is required")
	}
	if in.Scope == "" {
		return ExecuteResult{}, fmt.Errorf("idempotency scope is required")
	}
	if in.Key == "" {
		return ExecuteResult{}, fmt.Errorf("idempotency key is required")
	}
	if len(in.RequestHash) == 0 {
		return ExecuteResult{}, fmt.Errorf("idempotency request hash is required")
	}
	if in.Run == nil {
		return ExecuteResult{}, fmt.Errorf("idempotency run function is required")
	}
	if err := m.requireGuard(); err != nil {
		return ExecuteResult{}, err
	}

	cached, err := m.lookup(ctx, in.Token.ID, in.Scope, in.Key, in.RequestHash)
	if err != nil {
		return ExecuteResult{}, idempotencyLookupError(err)
	}
	if cached.found {
		if err := m.requireGuard(); err != nil {
			return ExecuteResult{}, err
		}
		return ExecuteResult{Status: cached.status, Body: cached.body, Replayed: true}, nil
	}

	release, err := m.acquireLock(ctx, in.Token.ID, in.Scope, in.Key)
	if err != nil {
		return ExecuteResult{}, fmt.Errorf("idempotency lock failed: %w", err)
	}
	defer release()
	if err := m.requireGuard(); err != nil {
		return ExecuteResult{}, err
	}

	cached, err = m.lookup(ctx, in.Token.ID, in.Scope, in.Key, in.RequestHash)
	if err != nil {
		return ExecuteResult{}, idempotencyLookupError(err)
	}
	if cached.found {
		if err := m.requireGuard(); err != nil {
			return ExecuteResult{}, err
		}
		return ExecuteResult{Status: cached.status, Body: cached.body, Replayed: true}, nil
	}
	if err := m.requireGuard(); err != nil {
		return ExecuteResult{}, err
	}

	override := &recordedResponseOverride{}
	disposition := &refusalDisposition{}
	callCtx := withRequest(ctx, Request{
		TokenID:     in.Token.ID,
		Scope:       in.Scope,
		Key:         in.Key,
		RequestHash: in.RequestHash,
	})
	callCtx = withRecordedResponseOverride(callCtx, override)
	callCtx = withRefusalDisposition(callCtx, disposition)
	status, body, err := in.Run(callCtx)
	if err != nil {
		return ExecuteResult{}, err
	}
	if status == 0 {
		status = http.StatusOK
	}
	// Mirror Wrap: a 5xx is an incomplete attempt, never pinned. For 4xx the
	// cache disposition is explicit, not status-derived: a handler that
	// committed nothing marks its refusal unconsumed, leaving the key usable
	// with a corrected body; every other 4xx is conservatively recorded like
	// a conclusion, because stateful refusals append authoritative events
	// before returning and MUST replay (and conflict on a changed body)
	// rather than re-execute.
	if status >= http.StatusInternalServerError {
		return ExecuteResult{Status: status, Body: body}, nil
	}
	if status >= http.StatusBadRequest && refusalUnconsumed(callCtx) {
		// Pure refusal: nothing committed, nothing to complete — the pin
		// advancing only means the stale process must not answer.
		if m.guardBlocked() {
			return ExecuteResult{}, m.requireGuard()
		}
		return ExecuteResult{Status: status, Body: body}, nil
	}
	if status >= http.StatusBadRequest {
		// Unmarked refusal: the handler may have committed authoritative
		// events while admitted/current. Its idempotency record is the
		// COMPLETION of that admitted mutation, so it must be durably
		// written even if the pin advanced after the commit — dropping it
		// would let the same key admit a second authoritative action after
		// cutover (IDEM-B4). The admission fence is scoped to exactly this
		// record append; the stale process still refuses outward below.
		if _, err := m.recordAdmitted(callCtx, in.Token, in.Scope, in.Key, in.RequestHash, status, body); err != nil {
			return ExecuteResult{}, fmt.Errorf("idempotency record failed: %w", err)
		}
		if m.guardBlocked() {
			return ExecuteResult{}, m.requireGuard()
		}
		cached, err = m.lookup(callCtx, in.Token.ID, in.Scope, in.Key, in.RequestHash)
		if err == nil && cached.found {
			return ExecuteResult{Status: cached.status, Body: cached.body}, nil
		}
		return ExecuteResult{Status: status, Body: body}, nil
	}
	recordBody := body
	hasOverride := false
	if overrideBody, ok := recordedResponse(callCtx); ok {
		recordBody = overrideBody
		hasOverride = true
	}
	fresh, err := m.record(callCtx, in.Token, in.Scope, in.Key, in.RequestHash, status, recordBody)
	if err != nil {
		// The handler was admitted only after a current-build check. If the
		// pin advances after its domain transaction commits, do not strand
		// the admitted response merely because the follow-on cache event is
		// now blocked. The retry remains safe under the domain idempotency
		// identity: re-execution converges on the same event rows.
		if m.guardBlocked() {
			return ExecuteResult{Status: status, Body: body}, nil
		}
		return ExecuteResult{}, fmt.Errorf("idempotency record failed: %w", err)
	}
	if hasOverride && fresh {
		return ExecuteResult{Status: status, Body: body}, nil
	}

	cached, err = m.lookup(callCtx, in.Token.ID, in.Scope, in.Key, in.RequestHash)
	if err == nil && cached.found {
		return ExecuteResult{Status: cached.status, Body: cached.body, Replayed: !fresh}, nil
	}
	return ExecuteResult{Status: status, Body: recordBody, Replayed: !fresh}, nil
}

func (m *Middleware) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			next.ServeHTTP(w, r)
			return
		}
		if m.writeGuardError(w) {
			return
		}
		tok, ok := auth.TokenFromContext(r.Context())
		if !ok {
			writeError(w, http.StatusUnauthorized, "missing_authenticated_token", "missing authenticated token")
			return
		}
		key := r.Header.Get(headerName)
		if key == "" {
			writeError(w, http.StatusBadRequest, "idempotency_key_required", "Idempotency-Key header is required")
			return
		}
		// The safety cap must bound this ReadAll, not just the handler's
		// decoder: this middleware buffers the body before any handler
		// runs, so an unbounded read here would let a caller occupy
		// arbitrary memory regardless of downstream limits.
		r.Body = http.MaxBytesReader(w, r.Body, safety.DefaultPolicy().MaxRequestBodyBytes)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			var maxErr *http.MaxBytesError
			if errors.As(err, &maxErr) {
				writeError(w, http.StatusRequestEntityTooLarge, "request_too_large", "request body exceeds resource safety limit")
				return
			}
			writeError(w, http.StatusBadRequest, "invalid_request_body", "could not read request body")
			return
		}
		_ = r.Body.Close()
		reqHash, err := requestHash(body)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_json", "request body must be valid JSON")
			return
		}
		scope := r.Method + " " + r.URL.Path

		// Fast path: try the unlocked lookup first. The steady state
		// of "client retry of an already-cached request" never needs
		// to pin a connection or wait on a lock.
		cached, err := m.lookup(r.Context(), tok.ID, scope, key, reqHash)
		if writeLookupError(w, err) {
			return
		}
		if cached.found {
			if m.writeGuardError(w) {
				return
			}
			writeCachedResponse(w, cached, "true")
			return
		}

		// Slow path: serialize concurrent first-seen requests with
		// the same (token_id, scope, key) on a Postgres session-level
		// advisory lock so only one runs the inner handler. The lock
		// is keyed without the body hash on purpose: different bodies
		// reusing one key must serialize too, so the second request
		// sees the cache row written by the first and surfaces the
		// 422 conflict from `lookup` rather than running its handler.
		release, err := m.acquireLock(r.Context(), tok.ID, scope, key)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "idempotency_lock_failed", "could not acquire idempotency lock")
			return
		}
		defer release()
		if m.writeGuardError(w) {
			return
		}

		// Re-check under the lock: a winner may have committed during
		// our acquisition wait. This is also where same-key /
		// different-body conflicts get caught before the handler is
		// allowed to mutate state.
		cached, err = m.lookup(r.Context(), tok.ID, scope, key, reqHash)
		if writeLookupError(w, err) {
			return
		}
		if cached.found {
			if m.writeGuardError(w) {
				return
			}
			writeCachedResponse(w, cached, "true")
			return
		}
		if m.writeGuardError(w) {
			return
		}

		rec := newRecorder()
		r.Body = io.NopCloser(bytes.NewReader(body))
		override := &recordedResponseOverride{}
		disposition := &refusalDisposition{}
		ctx := withRequest(r.Context(), Request{
			TokenID:     tok.ID,
			Scope:       scope,
			Key:         key,
			RequestHash: reqHash,
		})
		ctx = withRecordedResponseOverride(ctx, override)
		ctx = withRefusalDisposition(ctx, disposition)
		r = r.WithContext(ctx)
		next.ServeHTTP(rec, r)
		if rec.overflow {
			if m.writeGuardError(w) {
				return
			}
			writeError(w, http.StatusInternalServerError, "response_too_large",
				"response exceeds the deterministic API buffering limit")
			return
		}
		// 5xx responses are never recorded: the attempt did not complete
		// and a well-behaved same-key retry must re-execute. For 4xx the
		// cache disposition is explicit, not status-derived: a handler
		// that committed nothing marks its refusal unconsumed (validation,
		// not-found), which keeps the key usable with a corrected body.
		// Every unmarked 4xx is conservatively recorded like a conclusion:
		// stateful refusals (xylem/signal budget exhaustion) append
		// authoritative events before returning 409 and MUST replay — and
		// conflict on a changed body — rather than re-execute and append
		// again under one key.
		if rec.status >= http.StatusInternalServerError ||
			(rec.status >= http.StatusBadRequest && refusalUnconsumed(r.Context())) {
			if m.writeGuardError(w) {
				return
			}
			copyHeader(w.Header(), rec.header)
			w.WriteHeader(rec.status)
			_, _ = w.Write(rec.body.Bytes())
			return
		}
		if rec.status >= http.StatusBadRequest {
			// Unmarked refusal: the handler may have committed authoritative
			// events while admitted/current. Its idempotency record is the
			// COMPLETION of that admitted mutation and must be durably
			// written even if the pin advanced after the commit — dropping
			// it would let the same key admit a second authoritative action
			// after cutover (IDEM-B4). The admission fence covers exactly
			// this record append; a stale process still refuses outward
			// after the record is durable.
			if _, err := m.recordAdmitted(r.Context(), tok, scope, key, reqHash, rec.status, rec.body.Bytes()); err != nil {
				writeError(w, http.StatusInternalServerError, "idempotency_record_failed", "could not record idempotency key")
				return
			}
			if m.writeGuardError(w) {
				return
			}
			cached, err = m.lookup(r.Context(), tok.ID, scope, key, reqHash)
			if err == nil && cached.found {
				w.Header().Set("Content-Type", "application/json; charset=utf-8")
				w.WriteHeader(cached.status)
				_, _ = w.Write(cached.body)
				return
			}
			copyHeader(w.Header(), rec.header)
			w.WriteHeader(rec.status)
			_, _ = w.Write(rec.body.Bytes())
			return
		}

		recordBody := rec.body.Bytes()
		if overridden, ok := recordedResponse(r.Context()); ok {
			recordBody = overridden
		}
		fresh, err := m.record(r.Context(), tok, scope, key, reqHash, rec.status, recordBody)
		if err != nil {
			// Once the guarded pre-handler boundary admits a mutation, preserve
			// its first response if the pin advances after the domain commit. This
			// is load-bearing for one-time credentials such as subactor token
			// secrets: discarding the response would leave an active unusable token.
			if m.guardBlocked() {
				copyHeader(w.Header(), rec.header)
				w.WriteHeader(rec.status)
				_, _ = w.Write(rec.body.Bytes())
				return
			}
			writeError(w, http.StatusInternalServerError, "idempotency_record_failed", "could not record idempotency key")
			return
		}
		if override.body != nil && fresh {
			copyHeader(w.Header(), rec.header)
			w.WriteHeader(rec.status)
			_, _ = w.Write(rec.body.Bytes())
			return
		}

		// The winner serves its own response *re-fetched from the
		// cache row it just wrote*, not the in-memory rec.body. The
		// cache column is JSONB, which normalizes key order; replay
		// callers always read those normalized bytes, so the winner
		// must read them too — otherwise byte-equality between the
		// first and Nth caller breaks (semantically equivalent JSON
		// but visibly different to anything diffing the wire). The
		// winner's response loses no information: status and body
		// round-trip through the cache row.
		cached, err = m.lookup(r.Context(), tok.ID, scope, key, reqHash)
		if err == nil && cached.found {
			// fresh=true is the lock-protected normal path; fresh=
			// false should be unreachable but if a 64-bit lock-key
			// collision or out-of-band writer ever made it true,
			// "race" tells the caller their bytes came from a
			// different request's recording.
			replayedHeader := "true"
			if fresh {
				replayedHeader = ""
			}
			if replayedHeader != "" {
				w.Header().Set("Idempotency-Replayed", replayedHeader)
			}
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			w.WriteHeader(cached.status)
			_, _ = w.Write(cached.body)
			return
		}

		// Cache row vanished or lookup errored between record and
		// re-read (transient DB issue, TTL of zero, etc.). Fall back
		// to the buffered handler response so the caller still gets
		// a meaningful answer; future callers will repopulate the
		// cache.
		copyHeader(w.Header(), rec.header)
		w.WriteHeader(rec.status)
		_, _ = w.Write(rec.body.Bytes())
	})
}

func (m *Middleware) requireGuard() error {
	if m == nil || m.guard == nil {
		return nil
	}
	if err := m.guard(); err != nil {
		return fmt.Errorf("runtime guard blocked idempotent operation: %w", err)
	}
	return nil
}

func (m *Middleware) writeGuardError(w http.ResponseWriter) bool {
	if err := m.requireGuard(); err == nil {
		return false
	}
	writeError(w, http.StatusServiceUnavailable, "build_pin",
		"served build is not current; inspect /readyz for build status")
	return true
}

func (m *Middleware) guardBlocked() bool {
	return m != nil && m.guard != nil && m.guard() != nil
}

func idempotencyLookupError(err error) error {
	var conflict conflictError
	if errors.As(err, &conflict) {
		return fmt.Errorf("idempotency_key_conflict: idempotency key reused with a different request body")
	}
	return fmt.Errorf("idempotency lookup failed: %w", err)
}

// writeLookupError reports whether the lookup error has been turned
// into a final HTTP response. A nil error returns false (no response
// written, caller continues); any other error returns true and the
// caller must abort. Conflict errors are mapped to 422 here so both
// the fast-path and the post-lock recheck handle them uniformly.
func writeLookupError(w http.ResponseWriter, err error) bool {
	if err == nil {
		return false
	}
	var conflict conflictError
	if errors.As(err, &conflict) {
		writeError(w, http.StatusUnprocessableEntity, "idempotency_key_conflict", "idempotency key reused with a different request body")
		return true
	}
	writeError(w, http.StatusInternalServerError, "idempotency_lookup_failed", "could not check idempotency key")
	return true
}

func writeCachedResponse(w http.ResponseWriter, cached cachedResponse, replayedHeader string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Idempotency-Replayed", replayedHeader)
	w.WriteHeader(cached.status)
	_, _ = w.Write(cached.body)
}

// acquireLock takes a Postgres session-level advisory lock keyed on
// the request's idempotency identity, retrying with exponential
// backoff via pg_try_advisory_lock so that waiters never hold a pool
// connection between attempts. This matters because the inner
// handler that holds the lock also pulls connections from the same
// pool — using blocking pg_advisory_lock would let waiters starve the
// holder of conns and deadlock the entire request.
//
// The returned release func must always be called: it issues
// pg_advisory_unlock on a fresh short-timeout context and returns the
// connection to the pool, destroying the connection if the unlock
// itself fails (so a connection in unknown lock state never re-enters
// the pool). Callers should `defer release()` immediately.
func (m *Middleware) acquireLock(ctx context.Context, tokenID uuid.UUID, scope, key string) (func(), error) {
	lockKey := lockKeyFor(tokenID, scope, key)
	backoff := lockTryInitialBackoff
	for {
		conn, err := m.pool.Acquire(ctx)
		if err != nil {
			return nil, err
		}
		var got bool
		if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, lockKey).Scan(&got); err != nil {
			conn.Release()
			return nil, err
		}
		if got {
			return func() {
				unlockCtx, cancel := context.WithTimeout(context.Background(), unlockTimeout)
				defer cancel()
				if _, err := conn.Exec(unlockCtx, `SELECT pg_advisory_unlock($1)`, lockKey); err != nil {
					// Destroy the connection so a possibly-still-locked
					// backend never hands the lock to another caller via
					// the pool. Postgres releases session-level advisory
					// locks when the backend disconnects.
					_ = conn.Conn().Close(context.Background())
				}
				conn.Release()
			}, nil
		}
		// Lock held by someone else. Release the connection so the
		// holder's handler is not starved, then sleep before retry.
		conn.Release()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < lockTryMaxBackoff {
			backoff *= 2
			if backoff > lockTryMaxBackoff {
				backoff = lockTryMaxBackoff
			}
		}
	}
}

// lockKeyFor folds the idempotency identity into the int64 keyspace
// pg_advisory_lock takes. SHA-256 over the same canonical string the
// rest of the middleware uses; the leading 8 bytes are reinterpreted
// as int64 so the value covers the full Postgres bigint range
// (positive and negative). 64-bit collisions are theoretically
// possible — two unrelated identities serialize as a result, which is
// inefficient but never wrong. The body hash is intentionally NOT
// included: same-key / different-body requests must serialize so the
// lookup-after-lock can return 422.
func lockKeyFor(tokenID uuid.UUID, scope, key string) int64 {
	sum := sha256.Sum256([]byte(tokenID.String() + "|" + scope + "|" + key))
	return int64(binary.BigEndian.Uint64(sum[:8]))
}

type cachedResponse struct {
	found  bool
	status int
	body   []byte
}

type conflictError struct{}

func (conflictError) Error() string { return "idempotency conflict" }

func (m *Middleware) lookup(ctx context.Context, tokenID uuid.UUID, scope, key string, reqHash []byte) (cachedResponse, error) {
	var storedHash []byte
	var status int
	var body []byte
	err := m.pool.QueryRow(ctx, `
		SELECT request_hash, response_status, response_body
		FROM idempotency_keys
		WHERE token_id = $1 AND scope = $2 AND key = $3 AND expires_at > now()
	`, tokenID, scope, key).Scan(&storedHash, &status, &body)
	if errors.Is(err, pgx.ErrNoRows) {
		return cachedResponse{}, nil
	}
	if err != nil {
		return cachedResponse{}, err
	}
	if !bytes.Equal(storedHash, reqHash) {
		return cachedResponse{}, conflictError{}
	}
	return cachedResponse{found: true, status: status, body: body}, nil
}

// record persists the idempotency cache entry derived from the wrapped
// handler's response. It returns (fresh, error). fresh=false means
// some other writer beat us to the same (token_id, scope, key); under
// the advisory lock taken in Wrap that should never happen in v0, but
// we still return the bool so the middleware can fall back to serving
// the canonical cached bytes (belt-and-suspenders for lock-key
// collision, out-of-band writers, or session-lock loss across a
// connection bounce).
// recordAdmitted persists the idempotency record of an already-admitted
// stateful conclusion through the writer's enforced admitted-record path, so
// the record survives a reviewed-pin advance that happened after the
// handler's commit. Only the unmarked-4xx completion paths call this.
func (m *Middleware) recordAdmitted(ctx context.Context, tok domain.Token, scope, key string, reqHash []byte, status int, body []byte) (bool, error) {
	return m.recordWith(ctx, tok, scope, key, reqHash, status, body, m.writer.AppendAdmittedIdempotencyRecord)
}

func (m *Middleware) record(ctx context.Context, tok domain.Token, scope, key string, reqHash []byte, status int, body []byte) (bool, error) {
	return m.recordWith(ctx, tok, scope, key, reqHash, status, body, m.writer.Append)
}

func (m *Middleware) recordWith(ctx context.Context, tok domain.Token, scope, key string, reqHash []byte, status int, body []byte, appendFn func(context.Context, pgx.Tx, events.Spec) (uuid.UUID, bool, error)) (bool, error) {
	var decoded any
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	if err := dec.Decode(&decoded); err != nil {
		decoded = map[string]any{"raw": string(body)}
	}
	subjectID := uuid.NewSHA1(uuid.NameSpaceURL, []byte(tok.ID.String()+"|"+scope+"|"+key))
	tx, err := m.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	_, fresh, err := appendFn(ctx, tx, events.Spec{
		SubjectKind:  domain.SubjectIdempotencyKey,
		SubjectID:    subjectID,
		Kind:         domain.EventIdempotencyRecorded,
		Source:       tokenSource(tok),
		ActorTokenID: &tok.ID,
		Payload: map[string]any{
			"token_id":        tok.ID,
			"scope":           scope,
			"key":             key,
			"request_hash":    base64.StdEncoding.EncodeToString(reqHash),
			"response_status": status,
			"response_body":   decoded,
		},
	})
	if err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return fresh, nil
}

func tokenSource(tok domain.Token) domain.Source {
	if tok.Source.Valid() {
		return tok.Source
	}
	return domain.SourceHuman
}

func requestHash(body []byte) ([]byte, error) {
	if len(bytes.TrimSpace(body)) == 0 {
		body = []byte(`{}`)
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	canonical, err := events.CanonicalJSON(v)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(canonical)
	return sum[:], nil
}

type recorder struct {
	header   http.Header
	body     bytes.Buffer
	status   int
	overflow bool
}

func newRecorder() *recorder {
	return &recorder{header: make(http.Header), status: http.StatusOK}
}

func (r *recorder) Header() http.Header    { return r.header }
func (r *recorder) WriteHeader(status int) { r.status = status }
func (r *recorder) Write(b []byte) (int, error) {
	if r.overflow {
		return len(b), nil
	}
	if len(b) > safety.MaxBufferedAuthoritativeResponseBytes-r.body.Len() {
		r.overflow = true
		r.body.Reset()
		return len(b), nil
	}
	return r.body.Write(b)
}

func copyHeader(dst, src http.Header) {
	for k, values := range src {
		for _, value := range values {
			dst.Add(k, value)
		}
	}
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_, _ = fmt.Fprintf(w, `{"error":{"code":%q,"message":%q}}`+"\n", code, message)
}
