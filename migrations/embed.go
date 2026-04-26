// Package migrations exposes the on-disk SQL migration files as an embedded
// filesystem so the meristem binary is self-contained and can run migrations
// from inside the same container that serves the API.
//
// The on-disk layout is the source of truth; this file only re-exports it.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
