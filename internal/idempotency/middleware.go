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
}

func NewMiddleware(pool *pgxpool.Pool, writer *events.Writer) *Middleware {
	return &Middleware{pool: pool, writer: writer}
}

func (m *Middleware) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			next.ServeHTTP(w, r)
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
		body, err := io.ReadAll(r.Body)
		if err != nil {
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

		// Re-check under the lock: a winner may have committed during
		// our acquisition wait. This is also where same-key /
		// different-body conflicts get caught before the handler is
		// allowed to mutate state.
		cached, err = m.lookup(r.Context(), tok.ID, scope, key, reqHash)
		if writeLookupError(w, err) {
			return
		}
		if cached.found {
			writeCachedResponse(w, cached, "true")
			return
		}

		rec := newRecorder()
		r.Body = io.NopCloser(bytes.NewReader(body))
		override := &recordedResponseOverride{}
		ctx := withRequest(r.Context(), Request{
			TokenID:     tok.ID,
			Scope:       scope,
			Key:         key,
			RequestHash: reqHash,
		})
		ctx = withRecordedResponseOverride(ctx, override)
		r = r.WithContext(ctx)
		next.ServeHTTP(rec, r)

		recordBody := rec.body.Bytes()
		if overridden, ok := recordedResponse(r.Context()); ok {
			recordBody = overridden
		}
		fresh, err := m.record(r.Context(), tok, scope, key, reqHash, rec.status, recordBody)
		if err != nil {
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
func (m *Middleware) record(ctx context.Context, tok domain.Token, scope, key string, reqHash []byte, status int, body []byte) (bool, error) {
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
	_, fresh, err := m.writer.Append(ctx, tx, events.Spec{
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
	header http.Header
	body   bytes.Buffer
	status int
}

func newRecorder() *recorder {
	return &recorder{header: make(http.Header), status: http.StatusOK}
}

func (r *recorder) Header() http.Header         { return r.header }
func (r *recorder) WriteHeader(status int)      { r.status = status }
func (r *recorder) Write(b []byte) (int, error) { return r.body.Write(b) }

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
