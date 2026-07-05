package convergence

import (
	"testing"

	"github.com/google/uuid"
)

const testScribeCultivar = "convergence-scribe@1"

func TestValidateChecksProposalAcceptsClassifiedMachineAndHumanChecks(t *testing.T) {
	got := ValidateChecksProposal(ProposeChecksInput{
		ProposalOf: uuid.New(),
		Checks:     []string{"cmd:go test ./...", "query:parent_checks_defined", "human-ack:owner accepts"},
		Classified: []CheckClassification{
			{Check: "cmd:go test ./...", Class: "machine"},
			{Check: "query:parent_checks_defined", Class: "machine"},
			{Check: "human-ack:owner accepts", Class: "human"},
		},
	}, false, true)
	if !got.Accept {
		t.Fatalf("Accept = false, reason=%q", got.Reason)
	}
	if len(got.Checks) != 3 {
		t.Fatalf("normalized checks = %v, want 3", got.Checks)
	}
}

func TestValidateChecksProposalRefusals(t *testing.T) {
	cases := []struct {
		name string
		in   ProposeChecksInput
		want string
	}{
		{
			name: "empty",
			in:   ProposeChecksInput{},
			want: "empty_checks",
		},
		{
			name: "blank",
			in: ProposeChecksInput{
				Checks: []string{"cmd:go test", " "},
				Classified: []CheckClassification{
					{Check: "cmd:go test", Class: "machine"},
				},
			},
			want: "blank_check:1",
		},
		{
			name: "unprefixed",
			in: ProposeChecksInput{
				Checks: []string{"read the code"},
				Classified: []CheckClassification{
					{Check: "read the code", Class: "machine"},
				},
			},
			want: "unclassified_check:read the code",
		},
		{
			name: "unknown query",
			in: ProposeChecksInput{
				Checks: []string{"query:not_registered"},
				Classified: []CheckClassification{
					{Check: "query:not_registered", Class: "machine"},
				},
			},
			want: "unknown_query_check:not_registered",
		},
		{
			name: "duplicate",
			in: ProposeChecksInput{
				Checks: []string{"cmd:go test", "cmd:go test"},
				Classified: []CheckClassification{
					{Check: "cmd:go test", Class: "machine"},
				},
			},
			want: "duplicate_check:cmd:go test",
		},
		{
			name: "classification mismatch",
			in: ProposeChecksInput{
				Checks: []string{"human-ack:owner"},
				Classified: []CheckClassification{
					{Check: "human-ack:owner", Class: "machine"},
				},
			},
			want: "classification_mismatch:human-ack:owner",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ValidateChecksProposal(tc.in, false, true)
			if got.Accept {
				t.Fatalf("Accept = true, want false")
			}
			if got.Reason != tc.want {
				t.Fatalf("reason = %q, want %q", got.Reason, tc.want)
			}
		})
	}
}

func TestValidateChecksProposalRefusesAlreadyDefinedOrInvalidScribe(t *testing.T) {
	valid := ProposeChecksInput{
		ProposalOf: uuid.New(),
		Checks:     []string{"cmd:go test"},
		Classified: []CheckClassification{{Check: "cmd:go test", Class: "machine"}},
	}
	if got := ValidateChecksProposal(valid, true, true); got.Accept || got.Reason != "checks_already_defined" {
		t.Fatalf("already defined result = %+v", got)
	}
	if got := ValidateChecksProposal(valid, false, false); got.Accept || got.Reason != "invalid_scribe_child" {
		t.Fatalf("invalid child result = %+v", got)
	}
}

func TestChecksProposalSignalsDigestIncludesPayload(t *testing.T) {
	validation := checksProposalValidation{Accept: true, Reason: "ok", Checks: []string{"cmd:a"}}
	a, err := checksProposalSignals(proposalPayload{
		ProposalOf: uuid.New(),
		Checks:     []string{"cmd:a"},
		Classified: []CheckClassification{{Check: "cmd:a", Class: "machine"}},
		Cultivar:   testScribeCultivar,
	}, validation)
	if err != nil {
		t.Fatalf("signals a: %v", err)
	}
	b, err := checksProposalSignals(proposalPayload{
		ProposalOf: uuid.New(),
		Checks:     []string{"cmd:b"},
		Classified: []CheckClassification{{Check: "cmd:b", Class: "machine"}},
		Cultivar:   testScribeCultivar,
	}, validation)
	if err != nil {
		t.Fatalf("signals b: %v", err)
	}
	if len(a) != 2 || len(b) != 2 {
		t.Fatalf("signals lengths = %d/%d, want 2/2", len(a), len(b))
	}
	if len(a[1].Raw) != 64 || len(b[1].Raw) != 64 || a[1].Raw == b[1].Raw {
		t.Fatalf("payload digest should distinguish proposals: %q vs %q", a[1].Raw, b[1].Raw)
	}
}
