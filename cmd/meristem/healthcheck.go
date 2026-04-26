package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"
)

// runHealthcheck probes /readyz and exits 0 on HTTP 200, non-zero
// otherwise. The motivation is the Docker HEALTHCHECK directive on the
// distroless runtime image: distroless ships neither curl nor a shell,
// so the probe has to be a binary that already lives in the container.
//
// Probe target is derived in this order:
//
//  1. --url flag (highest priority; useful for ad-hoc operator probes
//     against another host).
//  2. http://127.0.0.1${MERISTEM_HTTP_ADDR}/readyz (the in-container
//     case; we always probe loopback because the api is in the same
//     network namespace as this command when invoked via HEALTHCHECK).
//  3. http://127.0.0.1:8080/readyz when MERISTEM_HTTP_ADDR is unset.
//
// On any non-2xx response or transport error, runHealthcheck returns
// an error with enough context to make the docker logs useful; main.go
// turns that into a non-zero process exit, which Docker reads as
// "unhealthy."
func runHealthcheck(ctx context.Context, _ *slog.Logger, args []string) error {
	fs := flag.NewFlagSet("healthcheck", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	urlFlag := fs.String("url", "", "URL to probe (default derived from MERISTEM_HTTP_ADDR + /readyz)")
	timeoutFlag := fs.Duration("timeout", 2*time.Second, "request timeout")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("healthcheck: %w", err)
	}

	target, err := resolveHealthcheckURL(*urlFlag, os.Getenv("MERISTEM_HTTP_ADDR"))
	if err != nil {
		return fmt.Errorf("healthcheck: %w", err)
	}

	probeCtx, cancel := context.WithTimeout(ctx, *timeoutFlag)
	defer cancel()

	req, err := http.NewRequestWithContext(probeCtx, http.MethodGet, target, nil)
	if err != nil {
		return fmt.Errorf("healthcheck: build request: %w", err)
	}

	client := &http.Client{Timeout: *timeoutFlag}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("healthcheck: GET %s: %w", target, err)
	}
	defer func() { _, _ = io.Copy(io.Discard, resp.Body); _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("healthcheck: %s returned HTTP %d", target, resp.StatusCode)
	}
	return nil
}

// resolveHealthcheckURL turns a possibly-empty --url and a possibly-
// empty MERISTEM_HTTP_ADDR into a concrete URL to probe. Lifted out of
// runHealthcheck so the parsing rules can be unit-tested without
// going through the network.
func resolveHealthcheckURL(urlFlag, addrEnv string) (string, error) {
	if urlFlag != "" {
		return urlFlag, nil
	}

	addr := addrEnv
	if addr == "" {
		addr = ":8080"
	}

	// MERISTEM_HTTP_ADDR follows net.Listen syntax: ":8080" (any iface),
	// "127.0.0.1:8080", "0.0.0.0:8080", etc. We always probe loopback;
	// the listening interface only determines what the api accepts
	// from the outside, not how the in-container probe reaches it.
	const probeHost = "127.0.0.1"
	switch {
	case strings.HasPrefix(addr, ":"):
		return fmt.Sprintf("http://%s%s/readyz", probeHost, addr), nil
	case strings.Contains(addr, ":"):
		port := addr[strings.LastIndex(addr, ":"):]
		return fmt.Sprintf("http://%s%s/readyz", probeHost, port), nil
	default:
		return "", errors.New("MERISTEM_HTTP_ADDR must be in host:port form (e.g. \":8080\")")
	}
}
