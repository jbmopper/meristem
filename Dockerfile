# wayline single-binary container.
#
# Multi-stage build:
#   - builder: alpine + Go toolchain, builds a fully static, stripped
#     binary using build/module caches mounted at build time.
#   - runtime: distroless static-debian12 (nonroot uid 65532). No shell,
#     no package manager, no setuid. The binary runs every wayline
#     subcommand (api, migrate, tokens, mcp, version), so the same image
#     covers the migration init container, the long-running api, and any
#     ad-hoc operator command.
#
# Migrations are embedded into the binary via embed.FS, so the runtime
# image needs no extra files.
#
# Default CMD is "api" so `docker run wayline` runs the HTTP server.
# Override at run time, e.g. `docker run wayline migrate`.
#
# syntax=docker/dockerfile:1.6
ARG GO_VERSION=1.25

FROM golang:${GO_VERSION}-alpine AS build
WORKDIR /src

# Cache module downloads independent of the source tree so source-only
# edits don't bust the dep layer.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

# VERSION is overridden at build time:
#   docker build --build-arg VERSION=$(git rev-parse --short HEAD) .
ARG VERSION=dev

# CGO_ENABLED=0 + -trimpath + -s -w -> small, reproducible, and
# transplantable into distroless static (which has no libc).
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build \
        -trimpath \
        -ldflags "-s -w -X main.version=${VERSION}" \
        -o /out/wayline \
        ./cmd/wayline

FROM gcr.io/distroless/static-debian12:nonroot AS runtime
COPY --from=build /out/wayline /wayline
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/wayline"]
CMD ["api"]
