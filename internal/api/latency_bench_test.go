package api

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/testutil/pgtest"
	"github.com/jbmopper/meristem/internal/workitems"
)

// Staged write-path benchmarks for work item 3b7202e0: attribute POST
// latency across (1) an event-writer append with no projectors, (2) the
// service layer (event append + synchronous projectors in one tx + response
// read-back), and (3) the full HTTP path (auth + idempotency
// fast-path/lock/record + handler), plus (4) the replay cache hit.
// Stage3-Stage2 is transport + idempotency overhead; Stage2-Stage1
// approximates projector + read-back cost. Run:
//
//	MERISTEM_INTEGRATION=1 MERISTEM_TEST_DATABASE_URL=... \
//	  go test ./internal/api/ -bench BenchmarkWritePath -run xxx -benchtime 50x
func BenchmarkWritePath(b *testing.B) {
	ctx := context.Background()
	pool := pgtest.NewPool(b, "meristem_bench")
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		b.Fatalf("migrate: %v", err)
	}
	writer := app.NewEventWriter()
	eventOnlyWriter := events.NewWriter(nil)
	authSvc := auth.NewService(pool, writer)
	rootResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name: "bench-root", IsRoot: true, Source: domain.SourceHuman,
	})
	if err != nil {
		b.Fatalf("root: %v", err)
	}
	root := rootResult.Token
	svc := workitems.NewService(pool, writer)
	server := New(pool, nil)

	post := func(path, key string, body []byte) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+rootResult.Secret)
		req.Header.Set("Idempotency-Key", key)
		req.Header.Set("Content-Type", "application/json; charset=utf-8")
		rec := httptest.NewRecorder()
		server.Handler().ServeHTTP(rec, req)
		return rec
	}

	b.Run("1_event_writer_no_projectors", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			tx, err := pool.Begin(ctx)
			if err != nil {
				b.Fatal(err)
			}
			if _, _, err = eventOnlyWriter.Append(ctx, tx, events.Spec{
				SubjectKind:  "work_item",
				SubjectID:    uuid.New(),
				Kind:         "work_item.event_appended",
				Source:       domain.SourceHuman,
				ActorTokenID: &root.ID,
				Payload: map[string]any{
					"inner_kind": "bench.noop",
					"inner":      map[string]any{"index": i},
				},
			}); err != nil {
				_ = tx.Rollback(ctx)
				b.Fatal(err)
			}
			_ = tx.Rollback(ctx)
		}
	})

	b.Run("2_service_create_with_projectors", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err := svc.Create(ctx, workitems.CreateInput{
				Title: fmt.Sprintf("bench service %d", i),
				Actor: root,
			}); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("3_full_http_post_with_idempotency", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			rec := post("/v1/work-items", fmt.Sprintf("bench-%d", i), []byte(fmt.Sprintf(`{"title":"bench http %d"}`, i)))
			if rec.Code != http.StatusCreated && rec.Code != http.StatusOK {
				b.Fatalf("status %d: %s", rec.Code, rec.Body.String())
			}
		}
	})

	b.Run("4_http_replay_cache_hit", func(b *testing.B) {
		body := []byte(`{"title":"bench replay"}`)
		_ = post("/v1/work-items", "bench-replay", body)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rec := post("/v1/work-items", "bench-replay", body)
			if rec.Code != http.StatusCreated && rec.Code != http.StatusOK {
				b.Fatalf("status %d", rec.Code)
			}
		}
	})
}
