package mcp

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
)

// protocolEra selects boundary validation and response rendering only. Both
// eras dispatch into the same tool handlers, access policy, idempotency, and
// projections; there is deliberately no era-specific business logic.
type protocolEra int

const (
	eraNone protocolEra = iota
	eraLegacy
	eraModern
)

func (e protocolEra) String() string {
	switch e {
	case eraLegacy:
		return "legacy"
	case eraModern:
		return "modern"
	default:
		return "none"
	}
}

const (
	// modernProtocolVersion is the sole 2026-core revision this server speaks.
	modernProtocolVersion = "2026-07-28"

	metaProtocolVersionKey    = "io.modelcontextprotocol/protocolVersion"
	metaClientCapabilitiesKey = "io.modelcontextprotocol/clientCapabilities"
	metaClientInfoKey         = "io.modelcontextprotocol/clientInfo"
	metaLogLevelKey           = "io.modelcontextprotocol/logLevel"
	metaServerInfoKey         = "io.modelcontextprotocol/serverInfo"
	// metaBuildKey carries the meristem build diagnostic on modern
	// server/discover results, reverse-DNS named per the spec's _meta rules.
	// The legacy initialize result keeps its historical top-level
	// meristemBuild field byte-for-byte.
	metaBuildKey = "com.jbmopper.meristem/build"
)

// EnvLegacyVersions may NARROW the served legacy protocol versions
// (comma-separated). Widening is impossible by construction: values outside
// the code-defined, fixture-backed set are ignored. A supported version is a
// support claim and lives in code next to its conformance fixtures.
const EnvLegacyVersions = "MERISTEM_MCP_LEGACY_VERSIONS"

// codeDefinedLegacyVersions is the complete fixture-backed legacy support
// set, newest first. Adding a version here requires adding its golden
// conformance coverage in era_legacy_test.go; runtime configuration cannot.
var codeDefinedLegacyVersions = []string{"2025-11-25", "2025-06-18"}

// legacyVersionsFromEnv resolves the served legacy set: the code-defined set,
// optionally narrowed by EnvLegacyVersions. Order is preserved newest first.
func legacyVersionsFromEnv() []string {
	raw := strings.TrimSpace(os.Getenv(EnvLegacyVersions))
	if raw == "" {
		return append([]string(nil), codeDefinedLegacyVersions...)
	}
	requested := make(map[string]bool)
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			requested[part] = true
		}
	}
	narrowed := make([]string, 0, len(codeDefinedLegacyVersions))
	for _, v := range codeDefinedLegacyVersions {
		if requested[v] {
			narrowed = append(narrowed, v)
		}
	}
	return narrowed
}

func (s *Server) legacyVersionSupported(version string) bool {
	for _, v := range s.legacyVersions {
		if v == version {
			return true
		}
	}
	return false
}

// legacyEnabled reports whether any legacy version survives narrowing. With
// an empty served set the legacy era is disabled entirely and every legacy
// opening fails closed naming the (modern-only) supported set.
func (s *Server) legacyEnabled() bool {
	return len(s.legacyVersions) > 0
}

// defaultLegacyVersion is the version served to openings that do not name a
// usable one (no proposal, unknown proposal, headerless compatibility). It is
// the oldest SERVED version — 2025-06-18 on an un-narrowed server, preserving
// the historical default — and never a version narrowing disabled. Callers
// must check legacyEnabled first; empty-set behavior is fail-closed.
func (s *Server) defaultLegacyVersion() string {
	if len(s.legacyVersions) == 0 {
		return ""
	}
	return s.legacyVersions[len(s.legacyVersions)-1]
}

// supportedVersions lists every protocol revision this process serves,
// modern first. This is the server/discover advertisement and the
// `supported` payload of every UnsupportedProtocolVersionError.
func (s *Server) supportedVersions() []string {
	return append([]string{modernProtocolVersion}, s.legacyVersions...)
}

// modernRequestMeta is the validated per-request metadata of a modern-era
// request. clientInfo is observational/compliance metadata only and never
// carries authority; invalid clientInfo is dropped, not rejected.
type modernRequestMeta struct {
	ProtocolVersion string
	ClientName      string
	ClientVersion   string
	LogLevel        string
}

// classifyModern inspects params._meta. shaped=true means the request carries
// the modern protocol-version key and MUST be handled as modern-era traffic:
// a malformed modern request is rejected, never downgraded to legacy.
func (s *Server) classifyModern(msg rpcMessage) (meta *modernRequestMeta, shaped bool, rerr *rpcError) {
	if len(msg.Params) == 0 {
		return nil, false, nil
	}
	var shell struct {
		Meta map[string]json.RawMessage `json:"_meta"`
	}
	if err := json.Unmarshal(msg.Params, &shell); err != nil || shell.Meta == nil {
		return nil, false, nil
	}
	versionRaw, ok := shell.Meta[metaProtocolVersionKey]
	if !ok {
		return nil, false, nil
	}
	out := &modernRequestMeta{}
	if err := json.Unmarshal(versionRaw, &out.ProtocolVersion); err != nil || strings.TrimSpace(out.ProtocolVersion) == "" {
		return out, true, rpcErrorf(errCodeInvalidParams,
			"_meta "+metaProtocolVersionKey+" must be a non-empty string")
	}
	capsRaw, ok := shell.Meta[metaClientCapabilitiesKey]
	if !ok {
		return out, true, rpcErrorf(errCodeInvalidParams,
			"missing required _meta field "+metaClientCapabilitiesKey)
	}
	// Reserved metadata values are schema-validated, not presence-checked:
	// clientCapabilities must be a ClientCapabilities object.
	var caps map[string]json.RawMessage
	if err := json.Unmarshal(capsRaw, &caps); err != nil || caps == nil {
		return out, true, rpcErrorf(errCodeInvalidParams,
			"_meta "+metaClientCapabilitiesKey+" must be a ClientCapabilities object")
	}
	if out.ProtocolVersion != modernProtocolVersion {
		return out, true, unsupportedProtocolError(out.ProtocolVersion, s.supportedVersions())
	}
	if infoRaw, ok := shell.Meta[metaClientInfoKey]; ok {
		// clientInfo stays optional and observational, but a present value
		// must be a valid Implementation (name and version required).
		var info struct {
			Name    string `json:"name"`
			Version string `json:"version"`
		}
		if err := json.Unmarshal(infoRaw, &info); err != nil || strings.TrimSpace(info.Name) == "" || strings.TrimSpace(info.Version) == "" {
			return out, true, rpcErrorf(errCodeInvalidParams,
				"_meta "+metaClientInfoKey+" must be an Implementation with name and version")
		}
		out.ClientName = info.Name
		out.ClientVersion = info.Version
	}
	if levelRaw, ok := shell.Meta[metaLogLevelKey]; ok {
		var level string
		if err := json.Unmarshal(levelRaw, &level); err != nil || !validLogLevel(level) {
			return out, true, rpcErrorf(errCodeInvalidParams,
				"_meta "+metaLogLevelKey+" must be a valid LoggingLevel")
		}
		out.LogLevel = level
	}
	return out, true, nil
}

// validLogLevel checks the RFC 5424 severities the MCP LoggingLevel schema
// defines.
func validLogLevel(level string) bool {
	switch level {
	case "debug", "info", "notice", "warning", "error", "critical", "alert", "emergency":
		return true
	default:
		return false
	}
}

func unsupportedProtocolError(requested string, supported []string) *rpcError {
	return rpcErrorWithData(errCodeUnsupportedProtocol,
		"unsupported protocol version",
		map[string]any{"supported": supported, "requested": requested})
}

// negotiateLegacyVersion applies the legacy initialize contract: answer a
// version from the SERVED set, never echo an unknown one and never fall back
// to a version narrowing disabled. Per the 2025-06-18 lifecycle specification
// a server that cannot support the requested version responds with one it
// does support and the client decides whether to continue. The empty string
// means legacy is disabled entirely (empty served set) and the caller must
// fail closed.
func (s *Server) negotiateLegacyVersion(raw json.RawMessage) string {
	if !s.legacyEnabled() {
		return ""
	}
	var params initializeParams
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &params)
	}
	version := params.ProtocolVersion
	if version == "" || !s.legacyVersionSupported(version) {
		version = s.defaultLegacyVersion()
	}
	return version
}

// logClassification is the protocol telemetry sink agreed for the 2026-core
// slice: structured slog with an allowlisted field set, no event kinds, no
// projections. It is the evidence stream for the legacy-removal gate, so the
// field names are a stable contract.
func (s *Server) logClassification(transport string, era protocolEra, requestedVersion, clientName, clientVersion, outcome string) {
	s.logger.Info("mcp protocol classification",
		slog.String("transport", transport),
		slog.String("era", era.String()),
		slog.String("requested_version", requestedVersion),
		slog.String("client_name", clientName),
		slog.String("client_version", clientVersion),
		slog.String("outcome", outcome))
}

// eraLockedError names the locked era so a mixed-era client gets a diagnostic
// rather than a silent failure; the only reset is a new connection/process.
func eraLockedError(locked protocolEra, lockedVersion string) *rpcError {
	detail := lockedVersion
	if detail == "" {
		detail = locked.String()
	}
	return rpcErrorf(errCodeInvalidRequest, fmt.Sprintf(
		"protocol era locked to %s (%s) for this connection; open a new connection to change eras",
		locked, detail))
}
