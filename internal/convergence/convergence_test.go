package convergence

import (
	"errors"
	"testing"

	"github.com/jbmopper/meristem/internal/domain"
)

func boolp(b bool) *bool      { return &b }
func f64p(f float64) *float64 { return &f }

func TestMajorityVote(t *testing.T) {
	cases := []struct {
		name    string
		signals []Signal
		want    domain.Verdict
	}{
		{
			name:    "no signals escalates",
			signals: nil,
			want:    domain.VerdictEscalate,
		},
		{
			name: "three of four passes accepts",
			signals: []Signal{
				{Kind: "grader.pass", Pass: boolp(true)},
				{Kind: "grader.pass", Pass: boolp(true)},
				{Kind: "grader.pass", Pass: boolp(true)},
				{Kind: "grader.pass", Pass: boolp(false)},
			},
			want: domain.VerdictAccept,
		},
		{
			name: "two of four ties and escalates",
			signals: []Signal{
				{Kind: "grader.pass", Pass: boolp(true)},
				{Kind: "grader.pass", Pass: boolp(true)},
				{Kind: "grader.pass", Pass: boolp(false)},
				{Kind: "grader.pass", Pass: boolp(false)},
			},
			want: domain.VerdictEscalate,
		},
		{
			name: "majority fail rejects",
			signals: []Signal{
				{Kind: "grader.pass", Pass: boolp(false)},
				{Kind: "grader.pass", Pass: boolp(false)},
				{Kind: "grader.pass", Pass: boolp(true)},
			},
			want: domain.VerdictReject,
		},
		{
			name: "score-only signals are ignored",
			signals: []Signal{
				{Kind: "grader.pass", Score: f64p(0.9)},
			},
			want: domain.VerdictEscalate,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v, err := MajorityVote{SignalKind: "grader.pass"}.Reduce(tc.signals)
			if err != nil {
				t.Fatalf("Reduce: %v", err)
			}
			if v.Disposition != tc.want {
				t.Fatalf("disposition = %q, want %q (reason: %s)", v.Disposition, tc.want, v.Reason)
			}
		})
	}
}

func TestUnanimous(t *testing.T) {
	all := []Signal{{Kind: "g", Pass: boolp(true)}, {Kind: "g", Pass: boolp(true)}}
	if v, _ := (Unanimous{SignalKind: "g"}).Reduce(all); v.Disposition != domain.VerdictAccept {
		t.Fatalf("all-pass should accept, got %q", v.Disposition)
	}
	one := []Signal{{Kind: "g", Pass: boolp(true)}, {Kind: "g", Pass: boolp(false)}}
	if v, _ := (Unanimous{SignalKind: "g"}).Reduce(one); v.Disposition != domain.VerdictReject {
		t.Fatalf("one-fail should reject, got %q", v.Disposition)
	}
	if v, _ := (Unanimous{SignalKind: "g"}).Reduce(nil); v.Disposition != domain.VerdictEscalate {
		t.Fatalf("no signals should escalate, got %q", v.Disposition)
	}
}

func TestThreshold(t *testing.T) {
	r := Threshold{SignalKind: "conf", Accept: 0.8}
	high := []Signal{{Kind: "conf", Score: f64p(0.9)}, {Kind: "conf", Score: f64p(0.7)}} // mean 0.8
	if v, _ := r.Reduce(high); v.Disposition != domain.VerdictAccept {
		t.Fatalf("mean 0.8 >= 0.8 should accept, got %q (%s)", v.Disposition, v.Reason)
	}
	low := []Signal{{Kind: "conf", Score: f64p(0.5)}}
	if v, _ := r.Reduce(low); v.Disposition != domain.VerdictReject {
		t.Fatalf("mean 0.5 < 0.8 should reject, got %q", v.Disposition)
	}
	if v, _ := r.Reduce(nil); v.Disposition != domain.VerdictEscalate {
		t.Fatalf("no scalar signals should escalate, got %q", v.Disposition)
	}
}

func TestAllPassChecklist(t *testing.T) {
	r := AllPassChecklist{Required: []string{"has_tests", "lint_clean"}}

	allPass := []Signal{
		{Kind: "checklist.item:has_tests", Pass: boolp(true)},
		{Kind: "checklist.item:lint_clean", Pass: boolp(true)},
	}
	if v, _ := r.Reduce(allPass); v.Disposition != domain.VerdictAccept {
		t.Fatalf("all required passing should accept, got %q (%s)", v.Disposition, v.Reason)
	}

	missing := []Signal{{Kind: "checklist.item:has_tests", Pass: boolp(true)}}
	if v, _ := r.Reduce(missing); v.Disposition != domain.VerdictReject {
		t.Fatalf("missing a check should reject, got %q", v.Disposition)
	}

	failing := []Signal{
		{Kind: "checklist.item:has_tests", Pass: boolp(true)},
		{Kind: "checklist.item:lint_clean", Pass: boolp(false)},
	}
	if v, _ := r.Reduce(failing); v.Disposition != domain.VerdictReject {
		t.Fatalf("a failing check should reject, got %q", v.Disposition)
	}

	if v, _ := (AllPassChecklist{}).Reduce(nil); v.Disposition != domain.VerdictEscalate {
		t.Fatalf("empty required set should escalate, got %q", v.Disposition)
	}
}

func TestAllPassChecklistFailureWins(t *testing.T) {
	// A check seen passing and failing must resolve to fail (strict).
	r := AllPassChecklist{Required: []string{"x"}}
	signals := []Signal{
		{Kind: "checklist.item:x", Pass: boolp(true)},
		{Kind: "checklist.item:x", Pass: boolp(false)},
	}
	if v, _ := r.Reduce(signals); v.Disposition != domain.VerdictReject {
		t.Fatalf("mixed readings should reject, got %q", v.Disposition)
	}
}

func TestRunPopulatesReductionAndDigestIsStable(t *testing.T) {
	signals := []Signal{
		{Kind: "grader.pass", Pass: boolp(true), Source: SignalSource{Model: "m", SampleID: "1"}},
		{Kind: "grader.pass", Pass: boolp(true)},
		{Kind: "grader.pass", Pass: boolp(false)},
	}
	red, err := Run(MajorityVote{SignalKind: "grader.pass"}, signals, 1)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if red.ReducerIdentity != "majority_vote" || red.ReducerVersion != 1 {
		t.Fatalf("reducer identity/version not recorded: %+v", red)
	}
	if red.Verdict.Disposition != domain.VerdictAccept {
		t.Fatalf("expected accept, got %q", red.Verdict.Disposition)
	}
	if red.InputsDigest == "" {
		t.Fatal("inputs digest empty")
	}
	if red.ReducerConfig["signal_kind"] != "grader.pass" {
		t.Fatalf("reducer config not recorded: %+v", red.ReducerConfig)
	}
	// Same signals → identical digest.
	red2, err := Run(MajorityVote{SignalKind: "grader.pass"}, signals, 2)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if red.InputsDigest != red2.InputsDigest {
		t.Fatalf("digest not stable: %q vs %q", red.InputsDigest, red2.InputsDigest)
	}
	redDifferentConfig, err := Run(MajorityVote{SignalKind: "other.pass"}, signals, 1)
	if err != nil {
		t.Fatalf("Run different config: %v", err)
	}
	if red.InputsDigest == redDifferentConfig.InputsDigest {
		t.Fatal("digest did not change when reducer config changed")
	}
	// Different signals → different digest.
	red3, _ := Run(MajorityVote{SignalKind: "grader.pass"}, signals[:2], 1)
	if red.InputsDigest == red3.InputsDigest {
		t.Fatal("digest collided across different signal sets")
	}
}

func TestRunRejectsBadAttempt(t *testing.T) {
	if _, err := Run(MajorityVote{}, nil, 0); err == nil {
		t.Fatal("attempt 0 should error")
	}
}

func TestRunNilReducer(t *testing.T) {
	if _, err := Run(nil, nil, 1); err == nil {
		t.Fatal("nil reducer should error")
	}
}

type errReducer struct{}

func (errReducer) Identity() string                 { return "err" }
func (errReducer) Version() int                     { return 1 }
func (errReducer) Reduce([]Signal) (Verdict, error) { return Verdict{}, errors.New("boom") }

func TestRunPropagatesReducerError(t *testing.T) {
	if _, err := Run(errReducer{}, nil, 1); err == nil {
		t.Fatal("reducer error should propagate")
	}
}

func TestEventPayloadRoundTripsFields(t *testing.T) {
	red, err := Run(Threshold{SignalKind: "conf", Accept: 0.5}, []Signal{
		{Kind: "conf", Score: f64p(0.9), Raw: "looks good", Source: SignalSource{Model: "m", PromptVersion: "v2"}},
	}, 3)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	payload := red.EventPayload()
	if payload["reducer_identity"] != "threshold_mean" {
		t.Fatalf("identity missing in payload: %v", payload["reducer_identity"])
	}
	if payload["attempt"] != 3 {
		t.Fatalf("attempt missing in payload: %v", payload["attempt"])
	}
	if payload["inputs_digest"] != red.InputsDigest {
		t.Fatal("digest mismatch in payload")
	}
	config, ok := payload["reducer_config"].(map[string]any)
	if !ok || config["accept"] != 0.5 || config["signal_kind"] != "conf" {
		t.Fatalf("reducer_config missing in payload: %#v", payload["reducer_config"])
	}
	verdict, ok := payload["verdict"].(map[string]any)
	if !ok || verdict["disposition"] != string(domain.VerdictAccept) {
		t.Fatalf("verdict not rendered: %v", payload["verdict"])
	}
	signals, ok := payload["signals"].([]any)
	if !ok || len(signals) != 1 {
		t.Fatalf("signals not rendered: %v", payload["signals"])
	}
}
