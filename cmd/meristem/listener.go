package main

// meristem listener — the idle/focused supervisor runtime (listener control
// plane, slice 3; docs/listener-control-plane.md).
//
// The effective state is DERIVED, never process-local truth:
//
//	START -> read registration (by stable name) and latest base policy
//	      -> read the principal's held assignments
//	no held assignment -> IDLE(policy_event_id)
//	      -> mint the policy-lens cursor at head H (mint-before-snapshot)
//	      -> snapshot open eligible demand (server-side, deterministic order)
//	      -> attempt candidates in order; a claim conflict is a pure skip
//	      -> consume the stream after H; every delivery re-runs the snapshot
//	claim succeeds -> FOCUSED(work_item_id, assignment_event_id)
//	      -> watch the assigned/addressed lane for the release of EXACTLY
//	         this assignment generation
//	release observed -> discard the focus cursor, re-derive with the LATEST
//	         base policy (registration re-read; revision-specific cursors)
//
// Mint-before-snapshot deliberately permits duplicates and prevents gaps: a
// demand at or before H is in the snapshot, one after H is in the stream, one
// racing both may appear twice — the idempotent claim and the assignment
// conflict reducer collapse it.
//
// Eligibility is evaluated SERVER-SIDE (the stored policy, the durable demand
// event). The demand stream is only a wake signal: its lens carries the
// policy's tree/kind predicates (safe narrowing) but never the actor/origin
// predicates — the feed actor predicate matches the event AUTHOR, and demand
// events are system-authored, so an origin lens on the stream would silently
// starve the listener. The candidates snapshot applies the full contract.
//
// The stable control lane is split by transport this release: assignment
// control and terminal handback are event-pushed on the assigned/addressed
// lane, while policy revisions, rebinding, and retirement are detected by a
// BOUNDED POLL of the registration read (listener.* kinds are classed admin
// and admin kinds are not projectable — an event-push control lens is an
// open design question flagged for review). Either signal interrupts the
// current phase and forces re-derivation; content lenses never replace it.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/domain"
)

func listenerUsage(out io.Writer) {
	fmt.Fprintf(out, `Usage: meristem listener --name <registration-name> [flags]

Run the idle/focused listener supervisor for a registered listener address.
The bearer token (MERISTEM_TOKEN or a token file) must be the listener's
currently bound principal credential.

Flags:
  --name        stable listener registration name (required)
  --api         meristem API base URL (default http://127.0.0.1:8080)
  --cursor-dir  directory for durable cursors (default ~/.meristem/listener/<registration-uuid>)
  --once        run one derivation pass (snapshot + claim attempts) and exit
                instead of streaming; prints the derived state
  --interval    reconnect backoff for dropped streams (default 2s)
`)
}

func runListener(ctx context.Context, logger *slog.Logger, args []string) error {
	fs := flag.NewFlagSet("listener", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	api := fs.String("api", "http://127.0.0.1:8080", "meristem API base URL")
	name := fs.String("name", "", "listener registration name")
	cursorDir := fs.String("cursor-dir", "", "directory for durable cursors")
	once := fs.Bool("once", false, "run one derivation pass and exit")
	interval := fs.Duration("interval", 2*time.Second, "reconnect backoff for dropped streams")
	if err := fs.Parse(args); err != nil {
		listenerUsage(os.Stderr)
		return err
	}
	if strings.TrimSpace(*name) == "" {
		listenerUsage(os.Stderr)
		return errors.New("listener: --name is required")
	}
	token, source, err := resolveFeedToken()
	if err != nil {
		return err
	}
	if source != "" && source != "MERISTEM_TOKEN" {
		fmt.Fprintf(os.Stderr, "listener: using token from %s\n", source)
	}
	sup := &listenerSupervisor{
		api:     strings.TrimRight(*api, "/"),
		token:   token,
		name:    *name,
		backoff: *interval,
		logger:  logger,
		http:    &http.Client{Timeout: 30 * time.Second},
	}
	// The default cursor directory is keyed by the VALIDATED registration
	// UUID, never the raw name (LCP3-R1-B3): a name is an object address, not
	// a filesystem path, and must not be able to escape or reshape the cursor
	// root. Resolving the registration first also fails fast on a typo'd
	// name. --cursor-dir remains the explicit operator override.
	dir := strings.TrimSpace(*cursorDir)
	if dir == "" {
		reg, err := sup.getListener(ctx)
		if err != nil {
			return err
		}
		home, err := os.UserHomeDir()
		if err != nil {
			return fmt.Errorf("listener: resolve home for cursor dir: %w", err)
		}
		dir = defaultListenerCursorDir(home, reg.ID)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("listener: create cursor dir: %w", err)
	}
	sup.cursorDir = dir
	if *once {
		return sup.runOnce(ctx, os.Stdout)
	}
	return sup.run(ctx)
}

// defaultListenerCursorDir keys the durable cursor root by the registration's
// validated UUID — a fixed-alphabet path component — so no listener NAME
// (which admin-side validation only requires to be non-empty) ever becomes a
// path component.
func defaultListenerCursorDir(home string, listenerID uuid.UUID) string {
	return filepath.Join(home, ".meristem", "listener", listenerID.String())
}

type listenerSupervisor struct {
	api       string
	token     string
	name      string
	cursorDir string
	backoff   time.Duration
	logger    *slog.Logger
	http      *http.Client
}

// listenerView is the wire shape of one /v1/listeners entry, narrowed to the
// fields the supervisor derives from.
type listenerView struct {
	ID               uuid.UUID       `json:"id"`
	Name             string          `json:"name"`
	PrincipalTokenID uuid.UUID       `json:"principal_token_id"`
	PolicyEventID    string          `json:"policy_event_id"`
	Policy           json.RawMessage `json:"policy"`
	RetiredAt        string          `json:"retired_at"`
}

type heldAssignment struct {
	WorkItemID        uuid.UUID  `json:"work_item_id"`
	AssignmentEventID uuid.UUID  `json:"assignment_event_id"`
	ExpiresAt         time.Time  `json:"expires_at"`
	ListenerID        *uuid.UUID `json:"listener_id"`
}

type demandCandidateView struct {
	DemandEventID  uuid.UUID `json:"demand_event_id"`
	DemandEventSeq int64     `json:"demand_event_seq"`
	WorkItemID     uuid.UUID `json:"work_item_id"`
	Capability     string    `json:"capability"`
}

// run is the outer derivation loop: every iteration re-derives IDLE or
// FOCUSED from durable state and runs that phase until something changes.
// Context cancellation is a clean shutdown, never an error — a request torn
// down mid-flight by the canceled context must not masquerade as a failure.
func (s *listenerSupervisor) run(ctx context.Context) error {
	err := s.runLoop(ctx)
	if ctx.Err() != nil {
		return nil
	}
	return err
}

func (s *listenerSupervisor) runLoop(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		reg, err := s.getListener(ctx)
		if err != nil {
			return err
		}
		if reg.RetiredAt != "" {
			s.logger.Info("listener retired; supervisor exiting", slog.String("name", s.name))
			return nil
		}
		held, err := s.heldAssignments(ctx, reg.ID)
		if err != nil {
			return err
		}
		if len(held) > 0 {
			if err := s.focused(ctx, reg, held[0]); err != nil {
				return err
			}
			continue
		}
		claimed, err := s.idle(ctx, reg)
		if err != nil {
			return err
		}
		if claimed != nil {
			s.logger.Info("claimed; entering focused",
				slog.String("work_item", claimed.WorkItemID.String()),
				slog.String("assignment_event", claimed.AssignmentEventID.String()))
		}
	}
}

// runOnce performs a single derivation pass — registration read, held
// assignments, and (when idle) one mint + snapshot + claim sweep — then
// reports the derived state. It never opens a stream; restart tests and
// operators use it to observe derivation directly.
func (s *listenerSupervisor) runOnce(ctx context.Context, out io.Writer) error {
	reg, err := s.getListener(ctx)
	if err != nil {
		return err
	}
	if reg.RetiredAt != "" {
		fmt.Fprintf(out, "state=retired listener=%s\n", reg.ID)
		return nil
	}
	held, err := s.heldAssignments(ctx, reg.ID)
	if err != nil {
		return err
	}
	if len(held) > 0 {
		fmt.Fprintf(out, "state=focused work_item=%s assignment_event=%s\n",
			held[0].WorkItemID, held[0].AssignmentEventID)
		return nil
	}
	if _, err := s.bootstrapDemandCursor(ctx, reg); err != nil {
		return err
	}
	claimed, err := s.claimSweep(ctx, reg)
	if err != nil {
		return err
	}
	if claimed != nil {
		fmt.Fprintf(out, "state=focused work_item=%s assignment_event=%s\n",
			claimed.WorkItemID, claimed.AssignmentEventID)
		return nil
	}
	fmt.Fprintf(out, "state=idle policy_event=%s\n", reg.PolicyEventID)
	return nil
}

// idle runs one IDLE phase: mint-before-snapshot, claim sweep, then stream
// under the policy lens until a claim succeeds, the control lane reports a
// policy revision, or the context ends. Returns the claimed assignment (nil
// when re-derivation should run without one).
func (s *listenerSupervisor) idle(ctx context.Context, reg listenerView) (*heldAssignment, error) {
	cursor, err := s.bootstrapDemandCursor(ctx, reg)
	if err != nil {
		return nil, err
	}
	if claimed, err := s.claimSweep(ctx, reg); err != nil || claimed != nil {
		return claimed, err
	}

	// Stream phase. Two watchers share one cancellation: the demand lens
	// wakes the claim sweep; the control lane interrupts on any control
	// event for this listener (policy revision, rebinding, retirement).
	phaseCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	claimedCh := make(chan *heldAssignment, 1)
	errCh := make(chan error, 2)

	go func() {
		errCh <- s.watchControlLane(phaseCtx, reg, cancel)
	}()
	go func() {
		client := s.feedClient(s.demandLensQuery(reg))
		last := cursor
		for {
			next, err := client.consumeStream(phaseCtx, last, func(ev sseEvent) error {
				claimed, err := s.claimSweep(phaseCtx, reg)
				if err != nil {
					return err
				}
				if claimed != nil {
					claimedCh <- claimed
					cancel()
				}
				return nil
			})
			if next != "" {
				last = next
				if err := saveCursorFile(s.demandCursorPath(reg), last); err != nil {
					errCh <- err
					return
				}
			}
			if phaseCtx.Err() != nil {
				errCh <- nil
				return
			}
			if err != nil && classifyWatchError(err) != watchErrTransient {
				errCh <- err
				return
			}
			select {
			case <-phaseCtx.Done():
				errCh <- nil
				return
			case <-time.After(s.backoff):
			}
		}
	}()

	var firstErr error
	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil && firstErr == nil {
			firstErr = err
			cancel()
		}
	}
	if firstErr != nil {
		return nil, firstErr
	}
	select {
	case claimed := <-claimedCh:
		return claimed, nil
	default:
		return nil, ctx.Err()
	}
}

// focused watches the assigned/addressed lane until EXACTLY this assignment
// generation is released (terminal handback, yield, or worker-owned expiry),
// then discards the focus cursor and returns for re-derivation under the
// latest base policy. The release might already be durable before the watch
// starts (restart into a released generation), so the projection is checked
// first and after every reconnect.
func (s *listenerSupervisor) focused(ctx context.Context, reg listenerView, held heldAssignment) error {
	s.logger.Info("focused", slog.String("work_item", held.WorkItemID.String()),
		slog.String("assignment_event", held.AssignmentEventID.String()))
	focusCursor := filepath.Join(s.cursorDir, "focus-"+held.AssignmentEventID.String()+".cursor")

	released, err := s.assignmentReleased(ctx, held)
	if err != nil {
		return err
	}
	for !released {
		if err := ctx.Err(); err != nil {
			return nil
		}
		client := s.feedClient(url.Values{"scope": []string{"assigned"}})
		last, err := loadCursorFile(focusCursor)
		if err != nil {
			return err
		}
		if last == "" {
			last, err = mintCursorWithRetry(ctx, s.logger, client, s.backoff)
			if err != nil {
				return err
			}
			if err := saveCursorFile(focusCursor, last); err != nil {
				return err
			}
			// The release may have landed between the projection check and
			// the cursor mint; re-check before waiting on the stream.
			if released, err = s.assignmentReleased(ctx, held); err != nil || released {
				break
			}
		}
		phaseCtx, cancel := context.WithCancel(ctx)
		next, streamErr := client.consumeStream(phaseCtx, last, func(ev sseEvent) error {
			if ev.Item.Kind != domain.EventWorkItemAssignmentReleased {
				return nil
			}
			var release struct {
				AssignmentEventID uuid.UUID `json:"assignment_event_id"`
			}
			if err := json.Unmarshal(ev.Item.Payload, &release); err == nil &&
				release.AssignmentEventID == held.AssignmentEventID {
				cancel()
			}
			return nil
		})
		cancel()
		if next != "" {
			if err := saveCursorFile(focusCursor, next); err != nil {
				return err
			}
		}
		if released, err = s.assignmentReleased(ctx, held); err != nil {
			return err
		}
		if released || ctx.Err() != nil {
			break
		}
		if streamErr != nil && classifyWatchError(streamErr) != watchErrTransient {
			return streamErr
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(s.backoff):
		}
	}
	// Discard the focus cursor only after the release is observed.
	if released {
		if err := os.Remove(focusCursor); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("listener: discard focus cursor: %w", err)
		}
		s.appendStatus(ctx, held.WorkItemID,
			fmt.Sprintf("listener %s observed release of assignment %s; returning to idle under the latest base policy", s.name, held.AssignmentEventID))
	}
	return nil
}

// assignmentReleased reads the durable projection: the generation is released
// when the item's current assignment no longer names it.
func (s *listenerSupervisor) assignmentReleased(ctx context.Context, held heldAssignment) (bool, error) {
	var payload struct {
		Assignment struct {
			AssignmentEventID uuid.UUID `json:"assignment_event_id"`
			HolderTokenID     uuid.UUID `json:"holder_token_id"`
		} `json:"assignment"`
	}
	status, err := s.getJSON(ctx, "/v1/work-items/"+held.WorkItemID.String()+"/assignment", &payload)
	if err != nil {
		return false, err
	}
	if status == http.StatusNotFound {
		return true, nil
	}
	if status != http.StatusOK {
		return false, fmt.Errorf("listener: read assignment: status %d", status)
	}
	return payload.Assignment.AssignmentEventID != held.AssignmentEventID, nil
}

// watchControlLane interrupts the current phase when the listener's durable
// control state changes: a policy revision, credential rebinding, or
// retirement observed on the registration. The outer loop then re-derives
// from durable state. This release detects revisions by BOUNDED POLL of the
// registration read (listener.* kinds are classed admin and admin kinds are
// deliberately not projectable, so there is no event-push lens for them yet
// — reclassifying them is an open control-plane design question, flagged for
// review); assignment control and terminal handback remain event-pushed on
// the assigned/addressed lane.
func (s *listenerSupervisor) watchControlLane(ctx context.Context, observed listenerView, interrupt func()) error {
	ticker := time.NewTicker(s.controlPoll())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
		current, err := s.getListener(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			s.logger.Warn("control poll failed", slog.String("error", err.Error()))
			continue
		}
		if current.PolicyEventID != observed.PolicyEventID ||
			current.RetiredAt != observed.RetiredAt ||
			current.PrincipalTokenID != observed.PrincipalTokenID {
			s.logger.Info("control state changed; re-deriving",
				slog.String("policy_event", current.PolicyEventID))
			interrupt()
			return nil
		}
	}
}

// controlPoll bounds how stale a policy revision can go unnoticed while a
// phase is streaming. Kept well above the stream backoff so quiet supervisors
// stay quiet.
func (s *listenerSupervisor) controlPoll() time.Duration {
	if poll := s.backoff * 10; poll > time.Second {
		return poll
	}
	return time.Second
}

// bootstrapDemandCursor mints the revision-specific demand cursor BEFORE any
// snapshot read (mint-before-snapshot: duplicates are collapsed by the claim
// reducer, gaps are impossible). The cursor file is keyed by policy revision,
// so a policy change abandons the old lens's queue deliberately.
func (s *listenerSupervisor) bootstrapDemandCursor(ctx context.Context, reg listenerView) (string, error) {
	path := s.demandCursorPath(reg)
	saved, err := loadCursorFile(path)
	if err != nil {
		return "", err
	}
	if saved != "" {
		return saved, nil
	}
	client := s.feedClient(s.demandLensQuery(reg))
	minted, err := mintCursorWithRetry(ctx, s.logger, client, s.backoff)
	if err != nil {
		return "", err
	}
	if err := saveCursorFile(path, minted); err != nil {
		return "", err
	}
	return minted, nil
}

func (s *listenerSupervisor) demandCursorPath(reg listenerView) string {
	revision := strings.TrimSpace(reg.PolicyEventID)
	if revision == "" {
		revision = "unset"
	}
	return filepath.Join(s.cursorDir, "demand-"+revision+".cursor")
}

// demandLensQuery is the STREAM lens: the dispatch projection narrowed by the
// policy's tree/kind predicates only. Origin (actor) predicates never reach
// the stream lens — the feed actor predicate matches the event AUTHOR and
// demand is system-authored — the server-side candidates snapshot applies
// the full contract on every wake.
func (s *listenerSupervisor) demandLensQuery(reg listenerView) url.Values {
	q := url.Values{}
	q.Set("projection", DemandStreamProjection)
	var policy struct {
		Predicates []struct {
			Kind       string   `json:"kind"`
			WorkItemID string   `json:"work_item_id"`
			EventKinds []string `json:"event_kinds"`
		} `json:"predicates"`
	}
	if len(reg.Policy) > 0 {
		if err := json.Unmarshal(reg.Policy, &policy); err == nil {
			for _, p := range policy.Predicates {
				switch p.Kind {
				case "work_item":
					q.Set("work_item", p.WorkItemID)
				case "work_item_tree":
					q.Set("work_item_tree", p.WorkItemID)
				case "kind_include":
					for _, k := range p.EventKinds {
						q.Add("kind", k)
					}
				case "kind_exclude":
					for _, k := range p.EventKinds {
						q.Add("exclude_kind", k)
					}
				}
			}
		}
	}
	return q
}

// DemandStreamProjection is the stream wake lens; candidates snapshots pin
// eligibility server-side against the same demand lane.
const DemandStreamProjection = "dispatch"

// claimSweep re-reads the server-side candidates snapshot and attempts each
// in deterministic order. A conflict or unavailability is a PURE skip — the
// key is preserved and another listener simply won — and any success is this
// listener's one assignment (max_concurrent_assignments is 1 this release).
func (s *listenerSupervisor) claimSweep(ctx context.Context, reg listenerView) (*heldAssignment, error) {
	var payload struct {
		Candidates []demandCandidateView `json:"candidates"`
	}
	status, err := s.getJSON(ctx, "/v1/listeners/"+reg.ID.String()+"/demand/candidates", &payload)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("listener: demand candidates: status %d", status)
	}
	for _, candidate := range payload.Candidates {
		claimed, ok, err := s.claim(ctx, reg, candidate)
		if err != nil {
			return nil, err
		}
		if ok {
			s.appendStatus(ctx, claimed.WorkItemID,
				fmt.Sprintf("listener %s claimed assignment %s for demand %s (capability %s)",
					s.name, claimed.AssignmentEventID, candidate.DemandEventID, candidate.Capability))
			return claimed, nil
		}
	}
	return nil, nil
}

// claim attempts one LISTENER-BOUND atomic claim: the server locks the
// registration and revalidates binding, policy revision, demand eligibility,
// actor authority, and capacity in the claim transaction — the supervisor's
// snapshot is only ever a suggestion. ok=false covers every pure refusal
// (held by a racer, stale policy, ineligible demand, at capacity, vanished)
// — the sweep just moves on; a stale-policy refusal surfaces on the next
// re-derivation.
func (s *listenerSupervisor) claim(ctx context.Context, reg listenerView, candidate demandCandidateView) (*heldAssignment, bool, error) {
	body, err := json.Marshal(map[string]string{
		"demand_event_id":          candidate.DemandEventID.String(),
		"observed_policy_event_id": reg.PolicyEventID,
	})
	if err != nil {
		return nil, false, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		s.api+"/v1/listeners/"+reg.ID.String()+"/claim", bytes.NewReader(body))
	if err != nil {
		return nil, false, err
	}
	req.Header.Set("Authorization", "Bearer "+s.token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", uuid.NewString())
	resp, err := s.http.Do(req)
	if err != nil {
		return nil, false, fmt.Errorf("listener: claim: %w", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	switch resp.StatusCode {
	case http.StatusOK, http.StatusCreated:
		var payload struct {
			Assignment heldAssignment `json:"assignment"`
		}
		if err := json.Unmarshal(raw, &payload); err != nil {
			return nil, false, fmt.Errorf("listener: decode claim: %w", err)
		}
		payload.Assignment.WorkItemID = candidate.WorkItemID
		return &payload.Assignment, true, nil
	case http.StatusConflict, http.StatusNotFound, http.StatusForbidden:
		return nil, false, nil
	default:
		return nil, false, newAPIRequestError(resp.StatusCode, raw)
	}
}

// appendStatus records worker-visible progress on the work item. Best-effort
// by design: a status write must never wedge the supervisor's state machine,
// so failures are logged and the loop continues.
func (s *listenerSupervisor) appendStatus(ctx context.Context, workItemID uuid.UUID, note string) {
	payload := map[string]any{
		"kind": "agent.status",
		"payload": map[string]any{
			"actor": "listener:" + s.name,
			"note":  note,
		},
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		s.api+"/v1/work-items/"+workItemID.String()+"/events", bytes.NewReader(raw))
	if err != nil {
		return
	}
	req.Header.Set("Authorization", "Bearer "+s.token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", uuid.NewString())
	resp, err := s.http.Do(req)
	if err != nil {
		s.logger.Warn("listener status append failed", slog.String("error", err.Error()))
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		s.logger.Warn("listener status append refused",
			slog.Int("status", resp.StatusCode), slog.String("body", strings.TrimSpace(string(body))))
	}
}

func (s *listenerSupervisor) getListener(ctx context.Context) (listenerView, error) {
	var payload struct {
		Listener listenerView `json:"listener"`
	}
	status, err := s.getJSON(ctx, "/v1/listeners/by-name/"+url.PathEscape(s.name), &payload)
	if err != nil {
		return listenerView{}, err
	}
	if status != http.StatusOK {
		return listenerView{}, fmt.Errorf("listener: resolve %q: status %d", s.name, status)
	}
	return payload.Listener, nil
}

// heldAssignments returns the assignments this LISTENER holds. Attribution
// is generation-bound to the listener, not just the token: the same principal
// backing multiple registrations (or holding manual unbound claims) must not
// resume another listener's — or a human's — lease.
func (s *listenerSupervisor) heldAssignments(ctx context.Context, listenerID uuid.UUID) ([]heldAssignment, error) {
	var payload struct {
		Assignments []heldAssignment `json:"assignments"`
	}
	status, err := s.getJSON(ctx, "/v1/assignments/held", &payload)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("listener: held assignments: status %d", status)
	}
	mine := payload.Assignments[:0]
	for _, a := range payload.Assignments {
		if a.ListenerID != nil && *a.ListenerID == listenerID {
			mine = append(mine, a)
		}
	}
	return mine, nil
}

func (s *listenerSupervisor) getJSON(ctx context.Context, path string, out any) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.api+path, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Authorization", "Bearer "+s.token)
	resp, err := s.http.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusOK {
		if err := json.Unmarshal(body, out); err != nil {
			return resp.StatusCode, fmt.Errorf("listener: decode %s: %w", path, err)
		}
	}
	return resp.StatusCode, nil
}

func (s *listenerSupervisor) feedClient(query url.Values) *feedClient {
	return &feedClient{
		baseURL:    s.api,
		token:      s.token,
		query:      query,
		http:       &http.Client{Timeout: 30 * time.Second},
		streamHTTP: &http.Client{},
	}
}
