package crossnode

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/peerhttp"
)

const maxOutcomeResponseBody = 2 << 20

var (
	ErrOutcomeSyncConfig   = errors.New("crossnode: invalid outcome sync configuration")
	ErrOutcomeSyncResponse = errors.New("crossnode: invalid outcome sync response")
)

type OutcomeSyncConfig struct {
	QueueHostNodeID string
	QueueHostOrigin string
	OriginNodeID    string
	QueueHostToken  string
	LocalActor      domain.Token
	RequestTimeout  time.Duration
	PageLimit       int
}

type OutcomeSyncService struct {
	cfg      OutcomeSyncConfig
	observer *OutcomeObserver
	client   *http.Client
}

func NewOutcomeSyncService(observer *OutcomeObserver, cfg OutcomeSyncConfig, options peerhttp.Options) (*OutcomeSyncService, error) {
	if observer == nil || !domain.ValidNodeID(cfg.QueueHostNodeID) || !domain.ValidNodeID(cfg.OriginNodeID) ||
		strings.TrimSpace(cfg.QueueHostToken) == "" || cfg.LocalActor.ID == uuid.Nil ||
		(cfg.LocalActor.Source != domain.SourceAgent && cfg.LocalActor.Source != domain.SourceSystem) ||
		AuthorizeOutcomeObserve(cfg.LocalActor, cfg.QueueHostNodeID, cfg.OriginNodeID) != nil {
		return nil, ErrOutcomeSyncConfig
	}
	origin, err := domain.CanonicalNodeOrigin(cfg.QueueHostOrigin)
	if err != nil {
		return nil, fmt.Errorf("%w: queue host origin", ErrOutcomeSyncConfig)
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = 5 * time.Second
	}
	if cfg.PageLimit <= 0 {
		cfg.PageLimit = defaultOutcomeLimit
	}
	if cfg.PageLimit > maxOutcomeLimit {
		cfg.PageLimit = maxOutcomeLimit
	}
	cfg.QueueHostOrigin = origin
	options.Timeout = cfg.RequestTimeout
	return &OutcomeSyncService{cfg: cfg, observer: observer, client: peerhttp.NewClient(options)}, nil
}

type OutcomeSyncResult struct {
	Cursor           int64
	Observed         int
	CauseTransitions int
}

// Tick performs one bounded outbound read. Any transport, authentication, or
// validation error happens before local mutation; a successfully observed page
// commits its evidence and cursor atomically.
func (s *OutcomeSyncService) Tick(ctx context.Context) (OutcomeSyncResult, error) {
	cursor, err := s.observer.Cursor(ctx, s.cfg.QueueHostNodeID, s.cfg.OriginNodeID)
	if err != nil {
		return OutcomeSyncResult{}, err
	}
	query := url.Values{}
	query.Set("origin", s.cfg.OriginNodeID)
	query.Set("after", strconv.FormatInt(cursor, 10))
	query.Set("limit", strconv.Itoa(s.cfg.PageLimit))
	requestCtx, cancel := context.WithTimeout(ctx, s.cfg.RequestTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(requestCtx, http.MethodGet, s.cfg.QueueHostOrigin+"/v1/crossnode/outcomes?"+query.Encode(), nil)
	if err != nil {
		return OutcomeSyncResult{}, fmt.Errorf("crossnode: build outcome request: %w", err)
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Authorization", "Bearer "+s.cfg.QueueHostToken)
	response, err := s.client.Do(request)
	if err != nil {
		return OutcomeSyncResult{}, fmt.Errorf("crossnode: fetch outcomes: %w", err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		return OutcomeSyncResult{}, fmt.Errorf("%w: status %d", ErrOutcomeSyncResponse, response.StatusCode)
	}
	limited := io.LimitReader(response.Body, maxOutcomeResponseBody+1)
	body, err := io.ReadAll(limited)
	if err != nil || len(body) > maxOutcomeResponseBody {
		return OutcomeSyncResult{}, fmt.Errorf("%w: unreadable or oversized body", ErrOutcomeSyncResponse)
	}
	var page struct {
		OriginNodeID string         `json:"origin_node_id"`
		NextCursor   int64          `json:"next_cursor"`
		Outcomes     []QueueOutcome `json:"outcomes"`
	}
	decoder := json.NewDecoder(strings.NewReader(string(body)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&page); err != nil {
		return OutcomeSyncResult{}, fmt.Errorf("%w: decode", ErrOutcomeSyncResponse)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return OutcomeSyncResult{}, fmt.Errorf("%w: trailing JSON", ErrOutcomeSyncResponse)
	}
	if page.OriginNodeID != s.cfg.OriginNodeID || page.NextCursor < cursor {
		return OutcomeSyncResult{}, ErrOutcomeSyncResponse
	}
	if len(page.Outcomes) == 0 {
		if page.NextCursor != cursor {
			return OutcomeSyncResult{}, ErrOutcomeSyncResponse
		}
	} else if page.NextCursor != page.Outcomes[len(page.Outcomes)-1].RemoteEventSeq {
		return OutcomeSyncResult{}, ErrOutcomeSyncResponse
	}
	observed, err := s.observer.Observe(ctx, ObserveOutcomesInput{
		QueueHostNodeID: s.cfg.QueueHostNodeID,
		LocalNodeID:     s.cfg.OriginNodeID,
		LocalActor:      s.cfg.LocalActor,
		Outcomes:        page.Outcomes,
	})
	if err != nil {
		return OutcomeSyncResult{}, err
	}
	return OutcomeSyncResult{
		Cursor:           observed.Cursor,
		Observed:         observed.Observed,
		CauseTransitions: observed.CauseTransitions,
	}, nil
}
