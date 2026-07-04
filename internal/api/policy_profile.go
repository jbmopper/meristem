package api

import (
	"errors"
	"net/http"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/policyprofile"
)

// canSwitchPolicyProfile gates POST /v1/policy-profile. The active profile is
// the owner's declared operating posture, so only human-source tokens may
// switch it — the same source rule as inbox capture, and for the same reason:
// agents are governed by this setting and must not author it.
func (s *Server) canSwitchPolicyProfile(w http.ResponseWriter, r *http.Request) bool {
	actor, ok := authenticatedToken(w, r)
	if !ok {
		return false
	}
	if actor.Source != "" && actor.Source != domain.SourceHuman {
		writeAPIError(w, http.StatusForbidden, "human_token_required", "policy profile switches require a human token")
		return false
	}
	return true
}

type switchPolicyProfileRequest struct {
	Profile string `json:"profile"`
}

type policyProfileResponse struct {
	Profile     string `json:"profile"`
	Fingerprint string `json:"fingerprint"`
	Switched    bool   `json:"switched"`
}

func (s *Server) handleSwitchPolicyProfile(w http.ResponseWriter, r *http.Request) {
	actor, ok := authenticatedToken(w, r)
	if !ok {
		return
	}
	var req switchPolicyProfileRequest
	if !decodeJSONRequest(w, r, &req) {
		return
	}
	if req.Profile == "" {
		writeAPIError(w, http.StatusBadRequest, "profile_required", "profile is required")
		return
	}
	active, switched, err := s.policyProfiles.Switch(r.Context(), policyprofile.SwitchInput{
		To:    req.Profile,
		Actor: actor,
	})
	if err != nil {
		if errors.Is(err, policyprofile.ErrHumanRequired) {
			writeAPIError(w, http.StatusForbidden, "human_token_required", "policy profile switches require a human token")
			return
		}
		writeAPIError(w, http.StatusUnprocessableEntity, "invalid_policy_profile", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, policyProfileResponse{
		Profile:     active.Name,
		Fingerprint: active.Fingerprint,
		Switched:    switched,
	})
}
