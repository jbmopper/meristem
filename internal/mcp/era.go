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
	if _, ok := shell.Meta[metaClientCapabilitiesKey]; !ok {
		return out, true, rpcErrorf(errCodeInvalidParams,
			"missing required _meta field "+metaClientCapabilitiesKey)
	}
	if out.ProtocolVersion != modernProtocolVersion {
		return out, true, unsupportedProtocolError(out.ProtocolVersion, s.supportedVersions())
	}
	if infoRaw, ok := shell.Meta[metaClientInfoKey]; ok {
		var info struct {
			Name    string `json:"name"`
			Version string `json:"version"`
		}
		if err := json.Unmarshal(infoRaw, &info); err == nil {
			out.ClientName = info.Name
			out.ClientVersion = info.Version
		}
	}
	if levelRaw, ok := shell.Meta[metaLogLevelKey]; ok {
		var level string
		if err := json.Unmarshal(levelRaw, &level); err == nil {
			out.LogLevel = level
		}
	}
	return out, true, nil
}

func unsupportedProtocolError(requested string, supported []string) *rpcError {
	return rpcErrorWithData(errCodeUnsupportedProtocol,
		"unsupported protocol version",
		map[string]any{"supported": supported, "requested": requested})
}

// negotiateLegacyVersion applies the legacy initialize contract: answer a
// supported version, never echo an unknown one. Per the 2025-06-18 lifecycle
// specification a server that cannot support the requested version responds
// with one it does support and the client decides whether to continue.
func (s *Server) negotiateLegacyVersion(raw json.RawMessage) string {
	var params initializeParams
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &params)
	}
	version := params.ProtocolVersion
	if version == "" {
		version = protocolVersion
	}
	if !s.legacyVersionSupported(version) {
		version = protocolVersion
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
