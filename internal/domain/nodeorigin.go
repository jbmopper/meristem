package domain

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
)

// ErrInvalidNodeOrigin marks a node route that is not a credential-safe,
// origin-only HTTP endpoint.
var ErrInvalidNodeOrigin = errors.New("invalid node origin")

// ValidateNodeOrigin accepts an origin-only HTTPS URL, plus plaintext loopback
// for local development. Paths, userinfo, queries, and fragments are forbidden
// so transports can append canonical API paths without ambiguity or credential
// leakage. Private/LAN HTTP must be placed behind TLS or an authenticated
// overlay; it is not silently trusted.
func ValidateNodeOrigin(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return ErrInvalidNodeOrigin
	}
	if u.Path != "" && u.Path != "/" {
		return ErrInvalidNodeOrigin
	}
	if u.Scheme == "https" {
		return nil
	}
	if u.Scheme != "http" {
		return ErrInvalidNodeOrigin
	}
	host := u.Hostname()
	if strings.EqualFold(host, "localhost") {
		return nil
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("%w: plaintext HTTP is loopback-only", ErrInvalidNodeOrigin)
	}
	return nil
}
