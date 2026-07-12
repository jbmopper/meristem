package peerhttp

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"
)

type staticResolver []netip.Addr

func (r staticResolver) LookupNetIP(context.Context, string, string) ([]netip.Addr, error) {
	return append([]netip.Addr(nil), r...), nil
}

func TestClientRejectsUnsafeOrMixedDNSBeforeDial(t *testing.T) {
	tests := [][]netip.Addr{
		{netip.MustParseAddr("10.0.0.2")},
		{netip.MustParseAddr("169.254.169.254")},
		{netip.MustParseAddr("127.0.0.1")},
		{netip.MustParseAddr("93.184.216.34"), netip.MustParseAddr("10.0.0.2")},
	}
	for _, answers := range tests {
		dialed := false
		client := NewClient(Options{
			Resolver: staticResolver(answers),
			DialContext: func(context.Context, string, string) (net.Conn, error) {
				dialed = true
				return nil, errors.New("unexpected dial")
			},
		})
		req, _ := http.NewRequest(http.MethodGet, "https://registry.example/v1/nodes/registry-snapshot", nil)
		_, err := client.Do(req)
		if !errors.Is(err, ErrUnsafeAddress) {
			t.Errorf("answers %v: err = %v", answers, err)
		}
		if dialed {
			t.Errorf("answers %v: unsafe resolution reached dial", answers)
		}
	}
}

func TestClientPinsApprovedResolution(t *testing.T) {
	var dialAddress string
	client := NewClient(Options{
		Resolver: staticResolver{netip.MustParseAddr("93.184.216.34")},
		DialContext: func(_ context.Context, _, address string) (net.Conn, error) {
			dialAddress = address
			return nil, errors.New("stop after observing pinned address")
		},
	})
	req, _ := http.NewRequest(http.MethodGet, "https://registry.example/v1/nodes/registry-snapshot", nil)
	_, _ = client.Do(req)
	if dialAddress != "93.184.216.34:443" {
		t.Fatalf("dial address = %q, want pinned address", dialAddress)
	}
}

func TestLoopbackNameCannotRebindToPublicAddress(t *testing.T) {
	client := NewClient(Options{
		Resolver: staticResolver{netip.MustParseAddr("93.184.216.34")},
		DialContext: func(context.Context, string, string) (net.Conn, error) {
			t.Fatal("widened localhost answer reached dial")
			return nil, nil
		},
	})
	req, _ := http.NewRequest(http.MethodGet, "http://localhost/v1/nodes/registry-snapshot", nil)
	_, err := client.Do(req)
	if !errors.Is(err, ErrUnsafeAddress) {
		t.Fatalf("err = %v", err)
	}
}

func TestClientAllowsExplicitLoopbackDevelopmentAndRefusesRedirect(t *testing.T) {
	if err := validateAddress(netip.MustParseAddr("127.0.0.1"), true); err != nil {
		t.Fatalf("explicit loopback rejected: %v", err)
	}
	client := NewClient(Options{})
	req, _ := http.NewRequest(http.MethodGet, "https://other.example/v1/nodes/registry-snapshot", nil)
	err := client.CheckRedirect(req, nil)
	if !errors.Is(err, ErrRedirectRefused) {
		t.Fatalf("redirect err = %v", err)
	}
}

func TestTransportRefusesNonCanonicalOrigin(t *testing.T) {
	client := NewClient(Options{})
	req, _ := http.NewRequest(http.MethodGet, "https://Registry.Example:443/v1/nodes/registry-snapshot", nil)
	_, err := client.Do(req)
	if !errors.Is(err, ErrOriginChanged) || !strings.Contains(err.Error(), "non-canonical") {
		t.Fatalf("err = %v", err)
	}
}

func TestPinnedDialPreservesTLSHostnameVerification(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}))
	defer server.Close()
	certificate := server.Certificate()
	if len(certificate.DNSNames) == 0 {
		t.Skip("httptest certificate has no DNS SAN")
	}
	_, port, err := net.SplitHostPort(server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(certificate)
	dialer := &net.Dialer{}
	client := NewClient(Options{
		Resolver:        staticResolver{netip.MustParseAddr("93.184.216.34")},
		TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12},
		DialContext: func(ctx context.Context, network, pinned string) (net.Conn, error) {
			if pinned != net.JoinHostPort("93.184.216.34", port) {
				t.Fatalf("pinned dial = %q", pinned)
			}
			return dialer.DialContext(ctx, network, server.Listener.Addr().String())
		},
	})
	request, _ := http.NewRequest(http.MethodGet, "https://"+net.JoinHostPort(certificate.DNSNames[0], port)+"/v1/nodes/registry-snapshot", nil)
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("TLS request through pinned IP: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", response.StatusCode)
	}
}
