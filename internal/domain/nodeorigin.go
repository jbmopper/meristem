package domain

import (
	"errors"
	"net"
	"net/url"
	"strconv"
	"strings"
)

// ErrInvalidNodeOrigin marks a node route that is not a credential-safe,
// origin-only HTTP endpoint.
var ErrInvalidNodeOrigin = errors.New("invalid node origin")

// ValidateNodeOrigin accepts an origin-only HTTPS URL, plus plaintext loopback
// for local development. Paths, userinfo, queries, and fragments are forbidden
// so transports can append canonical API paths without ambiguity or credential
// leakage. Private IP literals fail closed until a separate operator-approval
// mechanism exists; plaintext HTTP is loopback-only.
func ValidateNodeOrigin(raw string) error {
	_, err := CanonicalNodeOrigin(raw)
	return err
}

// CanonicalNodeOrigin validates raw and returns the single wire/persistence
// spelling used by node events and registry snapshots. Equivalent origins
// therefore have identical event payloads: scheme/host are lower-case,
// default ports and a root trailing slash are removed.
func CanonicalNodeOrigin(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" {
		return "", ErrInvalidNodeOrigin
	}
	if u.Path != "" && u.Path != "/" {
		return "", ErrInvalidNodeOrigin
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "https" && scheme != "http" {
		return "", ErrInvalidNodeOrigin
	}
	host := strings.ToLower(strings.TrimSuffix(u.Hostname(), "."))
	if host == "" {
		return "", ErrInvalidNodeOrigin
	}
	switch host {
	case "metadata", "metadata.google.internal", "metadata.goog", "instance-data.ec2.internal":
		return "", ErrInvalidNodeOrigin
	}
	port := u.Port()
	if port != "" {
		p, err := strconv.ParseUint(port, 10, 16)
		if err != nil || p == 0 {
			return "", ErrInvalidNodeOrigin
		}
		if (scheme == "https" && port == "443") || (scheme == "http" && port == "80") {
			port = ""
		}
	}
	ip := net.ParseIP(host)
	if ip != nil && (ip.IsUnspecified() || ip.IsMulticast() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsPrivate()) {
		return "", ErrInvalidNodeOrigin
	}
	if scheme == "http" && !strings.EqualFold(host, "localhost") && (ip == nil || !ip.IsLoopback()) {
		return "", ErrInvalidNodeOrigin
	}

	authority := host
	if strings.Contains(host, ":") {
		authority = "[" + host + "]"
	}
	if port != "" {
		authority = net.JoinHostPort(host, port)
	}
	return scheme + "://" + authority, nil
}
