// Package meristem is a small Go client for the meristem coordination
// plane. It hides the bits that every integrator would otherwise have
// to reinvent: bearer authentication, the Idempotency-Key dance,
// idempotency-replay detection, and decoding the API's structured
// error envelope.
//
// Design choices worth knowing:
//
//   - This package never validates the work_spec body against the JSON
//     Schema at docs/schemas/meristem.work_spec.v1.json. The server is
//     the single source of truth for that contract; the client merely
//     transports bytes. Callers pass any json-encodable shape as the
//     WorkSpec field.
//
//   - The package name is "meristem". When this collides with a local
//     identifier in the caller's package, use an import alias.
//
//   - The Client is safe for concurrent use; the underlying
//     *http.Client is too.
//
// Minimal example:
//
//	client, err := meristem.New(meristem.Config{
//	    BaseURL: "https://meristem.example.com",
//	    Token:   os.Getenv("MERISTEM_TOKEN"),
//	})
//	if err != nil { log.Fatal(err) }
//
//	resp, err := client.PostSignal(ctx, meristem.SignalRequest{
//	    Kind:      "repairable_failure",
//	    DedupeKey: "jay:retry-budget:001",
//	    Source: meristem.SignalSource{
//	        Kind:       "system_event",
//	        Identifier: "jay:job:42",
//	    },
//	    WorkSpec: workSpecJSON,
//	}, meristem.WithIdempotencyKey("import-001"))
//	if err != nil { log.Fatal(err) }
//	log.Printf("work_item=%s replayed=%v", resp.WorkItem.ID, resp.Replayed)
package meristem

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// defaultHTTPTimeout is the per-request deadline applied when the
// caller does not supply their own *http.Client. Conservative because
// /v1/signals is small and synchronous; integrators who need bigger
// uploads should supply their own client.
const defaultHTTPTimeout = 30 * time.Second

// Config configures a Client. BaseURL and Token are required; the
// rest fall back to sensible defaults.
type Config struct {
	// BaseURL is the API root, with no trailing /v1. Examples:
	// "https://meristem.example.com", "http://127.0.0.1:8080".
	BaseURL string

	// Token is a meristem bearer token (the secret shown once at
	// `meristem tokens create` time, not the token id). Sent as
	// "Authorization: Bearer <Token>".
	Token string

	// HTTPClient is optional. If nil, a fresh *http.Client with a
	// 30s timeout is used. Pass your own to share connection pools,
	// inject middleware, or change timeouts.
	HTTPClient *http.Client

	// UserAgent, if non-empty, is sent as the User-Agent header.
	// Recommended format: "your-service/version (contact)".
	UserAgent string
}

// Client is a meristem API client. Construct one with New.
type Client struct {
	baseURL   string
	token     string
	http      *http.Client
	userAgent string
}

// Errors returned by New for invalid configs.
var (
	ErrBaseURLRequired = errors.New("meristem: Config.BaseURL is required")
	ErrTokenRequired   = errors.New("meristem: Config.Token is required")
)

// New validates cfg and returns a ready-to-use Client. Returns a
// non-nil error if BaseURL or Token is empty; the rest of cfg is
// taken as-is.
func New(cfg Config) (*Client, error) {
	if cfg.BaseURL == "" {
		return nil, ErrBaseURLRequired
	}
	if cfg.Token == "" {
		return nil, ErrTokenRequired
	}
	httpc := cfg.HTTPClient
	if httpc == nil {
		httpc = &http.Client{Timeout: defaultHTTPTimeout}
	}
	return &Client{
		baseURL:   strings.TrimRight(cfg.BaseURL, "/"),
		token:     cfg.Token,
		http:      httpc,
		userAgent: cfg.UserAgent,
	}, nil
}

// APIError is returned when the meristem API responds with a 4xx or
// 5xx and a structured error envelope. StatusCode is the HTTP status;
// Code and Message come from the `{"error": {"code": ..., "message":
// ...}}` envelope. If the server returned a non-2xx response without
// the envelope (rare; usually a proxy in the path), Code will be
// "unknown" and Message will describe the status.
type APIError struct {
	StatusCode int    `json:"-"`
	Code       string `json:"code"`
	Message    string `json:"message"`
}

// Error implements error.
func (e *APIError) Error() string {
	return fmt.Sprintf("meristem: HTTP %d: %s: %s", e.StatusCode, e.Code, e.Message)
}

// Is supports errors.Is matching by code, e.g.:
//
//	errors.Is(err, &meristem.APIError{Code: "duplicate_work_spec"})
//
// StatusCode and Message are ignored for matching.
func (e *APIError) Is(target error) bool {
	other, ok := target.(*APIError)
	if !ok {
		return false
	}
	return e.Code == other.Code
}
