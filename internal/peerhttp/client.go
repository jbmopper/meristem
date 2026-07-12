// Package peerhttp provides the credential-safe HTTP transport used between
// meristem nodes. It resolves and validates the selected origin for every
// request, then dials only that approved address set. The request URL remains
// unchanged, so TLS continues to verify the registry hostname rather than a
// substituted IP address.
package peerhttp

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/jbmopper/meristem/internal/domain"
)

var (
	ErrUnsafeAddress     = errors.New("peerhttp: unsafe resolved address")
	ErrOriginChanged     = errors.New("peerhttp: request origin changed")
	ErrRedirectRefused   = errors.New("peerhttp: redirects are forbidden")
	ErrNoResolvedAddress = errors.New("peerhttp: origin resolved no addresses")
)

// Resolver is the narrow net.Resolver seam used by the pinned transport.
type Resolver interface {
	LookupNetIP(context.Context, string, string) ([]netip.Addr, error)
}

// DialContextFunc matches net.Dialer's DialContext method. It is exposed in
// Options for deterministic transport tests; production callers leave it nil.
type DialContextFunc func(context.Context, string, string) (net.Conn, error)

// Options configures a credential-safe peer client. Timeout bounds the whole
// request. Resolver, DialContext, and TLSClientConfig are primarily test seams.
type Options struct {
	Timeout         time.Duration
	Resolver        Resolver
	DialContext     DialContextFunc
	TLSClientConfig *tls.Config
}

// NewClient returns an HTTP client that refuses redirects and whose transport
// DNS-pins each request. A zero Timeout defaults to five seconds.
func NewClient(opts Options) *http.Client {
	if opts.Timeout <= 0 {
		opts.Timeout = 5 * time.Second
	}
	if opts.Resolver == nil {
		opts.Resolver = net.DefaultResolver
	}
	if opts.DialContext == nil {
		dialer := &net.Dialer{Timeout: opts.Timeout, KeepAlive: 30 * time.Second}
		opts.DialContext = dialer.DialContext
	}
	return &http.Client{
		Timeout:   opts.Timeout,
		Transport: &Transport{opts: opts},
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return ErrRedirectRefused
		},
	}
}

// Transport implements request-scoped DNS validation and pinning. It creates
// a no-keepalive standard-library transport per RoundTrip so a later request
// cannot inherit a connection selected under a different DNS answer.
type Transport struct {
	opts Options
}

func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req == nil || req.URL == nil {
		return nil, fmt.Errorf("peerhttp: nil request")
	}
	origin, err := requestOrigin(req.URL)
	if err != nil {
		return nil, err
	}
	canonical, err := domain.CanonicalNodeOrigin(origin)
	if err != nil || canonical != origin {
		return nil, fmt.Errorf("%w: invalid or non-canonical origin", ErrOriginChanged)
	}

	host := req.URL.Hostname()
	port := req.URL.Port()
	if port == "" {
		if req.URL.Scheme == "https" {
			port = "443"
		} else {
			port = "80"
		}
	}
	addresses, err := t.resolve(req.Context(), req.URL.Scheme, host)
	if err != nil {
		return nil, err
	}

	tlsConfig := t.opts.TLSClientConfig
	if tlsConfig != nil {
		tlsConfig = tlsConfig.Clone()
	}
	transport := &http.Transport{
		Proxy:               nil,
		TLSClientConfig:     tlsConfig,
		ForceAttemptHTTP2:   true,
		DisableKeepAlives:   true,
		TLSHandshakeTimeout: t.opts.Timeout,
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			requestedHost, requestedPort, splitErr := net.SplitHostPort(address)
			if splitErr != nil || !strings.EqualFold(strings.TrimSuffix(requestedHost, "."), strings.TrimSuffix(host, ".")) || requestedPort != port {
				return nil, ErrOriginChanged
			}
			var errs []error
			for _, ip := range addresses {
				conn, dialErr := t.opts.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
				if dialErr == nil {
					return conn, nil
				}
				errs = append(errs, dialErr)
			}
			return nil, errors.Join(errs...)
		},
	}
	return transport.RoundTrip(req)
}

func requestOrigin(u *url.URL) (string, error) {
	if u.Scheme == "" || u.Host == "" || u.User != nil {
		return "", fmt.Errorf("%w: missing origin", ErrOriginChanged)
	}
	return strings.ToLower(u.Scheme) + "://" + u.Host, nil
}

func (t *Transport) resolve(ctx context.Context, scheme, host string) ([]netip.Addr, error) {
	allowLoopback := strings.EqualFold(strings.TrimSuffix(host, "."), "localhost")
	if literal, err := netip.ParseAddr(host); err == nil {
		literal = literal.Unmap()
		allowLoopback = literal.IsLoopback()
		if err := validateAddress(literal, allowLoopback); err != nil {
			return nil, err
		}
		return []netip.Addr{literal}, nil
	}

	resolved, err := t.opts.Resolver.LookupNetIP(ctx, "ip", host)
	if err != nil {
		return nil, fmt.Errorf("peerhttp: resolve origin: %w", err)
	}
	if len(resolved) == 0 {
		return nil, ErrNoResolvedAddress
	}
	unique := make(map[netip.Addr]struct{}, len(resolved))
	for _, raw := range resolved {
		addr := raw.Unmap()
		if err := validateAddress(addr, allowLoopback); err != nil {
			return nil, err
		}
		unique[addr] = struct{}{}
	}
	addresses := make([]netip.Addr, 0, len(unique))
	for addr := range unique {
		addresses = append(addresses, addr)
	}
	sort.Slice(addresses, func(i, j int) bool { return addresses[i].Less(addresses[j]) })
	_ = scheme // scheme has already been validated by CanonicalNodeOrigin.
	return addresses, nil
}

var deniedPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("fec0::/10"),
	netip.MustParsePrefix("2001:db8::/32"),
}

func validateAddress(addr netip.Addr, allowLoopback bool) error {
	if allowLoopback && !addr.IsLoopback() {
		return fmt.Errorf("%w: loopback origin resolved outside loopback: %s", ErrUnsafeAddress, addr)
	}
	if !addr.IsValid() || addr.IsUnspecified() || addr.IsMulticast() || addr.IsLinkLocalUnicast() || addr.IsPrivate() || (!allowLoopback && addr.IsLoopback()) {
		return fmt.Errorf("%w: %s", ErrUnsafeAddress, addr)
	}
	if addr.IsLoopback() {
		return nil
	}
	for _, prefix := range deniedPrefixes {
		if prefix.Contains(addr) {
			return fmt.Errorf("%w: %s", ErrUnsafeAddress, addr)
		}
	}
	return nil
}
