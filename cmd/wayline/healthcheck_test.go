package main

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestResolveHealthcheckURL(t *testing.T) {
	cases := []struct {
		name    string
		urlFlag string
		addrEnv string
		want    string
		wantErr bool
	}{
		{name: "explicit url wins", urlFlag: "http://other:9000/readyz", addrEnv: ":8080", want: "http://other:9000/readyz"},
		{name: "default port when nothing set", urlFlag: "", addrEnv: "", want: "http://127.0.0.1:8080/readyz"},
		{name: "any-interface short form", urlFlag: "", addrEnv: ":9090", want: "http://127.0.0.1:9090/readyz"},
		{name: "explicit interface preserves port", urlFlag: "", addrEnv: "0.0.0.0:9090", want: "http://127.0.0.1:9090/readyz"},
		{name: "loopback host preserves port", urlFlag: "", addrEnv: "127.0.0.1:9090", want: "http://127.0.0.1:9090/readyz"},
		{name: "missing port rejected", urlFlag: "", addrEnv: "wayline", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveHealthcheckURL(tc.urlFlag, tc.addrEnv)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("want error; got url=%q", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("url mismatch: got=%q want=%q", got, tc.want)
			}
		})
	}
}

func TestRunHealthcheck(t *testing.T) {
	cases := []struct {
		name    string
		handler http.HandlerFunc
		wantErr bool
		errSubs string
	}{
		{
			name:    "200 is healthy",
			handler: func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) },
		},
		{
			name:    "503 is unhealthy",
			handler: func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) },
			wantErr: true,
			errSubs: "HTTP 503",
		},
		{
			name:    "404 is unhealthy",
			handler: func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNotFound) },
			wantErr: true,
			errSubs: "HTTP 404",
		},
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(tc.handler)
			defer srv.Close()
			err := runHealthcheck(context.Background(), logger, []string{"--url", srv.URL + "/readyz", "--timeout", "1s"})
			if tc.wantErr {
				if err == nil {
					t.Fatal("want error; got nil")
				}
				if tc.errSubs != "" && !strings.Contains(err.Error(), tc.errSubs) {
					t.Fatalf("error %q does not contain %q", err.Error(), tc.errSubs)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestRunHealthcheckUnreachable(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	// 127.0.0.1:1 is reserved and refuses connection on every machine
	// I have ever met; if your machine somehow listens here, this test
	// will spuriously pass.
	err := runHealthcheck(context.Background(), logger, []string{"--url", "http://127.0.0.1:1/readyz", "--timeout", "200ms"})
	if err == nil {
		t.Fatal("want connection error; got nil")
	}
}
