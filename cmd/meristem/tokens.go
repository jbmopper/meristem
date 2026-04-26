package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/storage"
)

func runTokens(ctx context.Context, logger *slog.Logger, args []string) error {
	if len(args) == 0 {
		tokensUsage(os.Stderr)
		return fmt.Errorf("tokens: missing subcommand")
	}
	cfg, err := storage.LoadConfigFromEnv()
	if err != nil {
		return err
	}
	pool, err := storage.Open(ctx, cfg)
	if err != nil {
		return err
	}
	defer pool.Close()

	writer := app.NewEventWriter()
	service := auth.NewService(pool, writer)

	switch args[0] {
	case "create":
		return runTokensCreate(ctx, service, args[1:])
	case "list":
		return runTokensList(ctx, service, os.Stdout)
	case "revoke":
		return runTokensRevoke(ctx, service, args[1:])
	default:
		logger.Error("unknown tokens subcommand", slog.String("subcommand", args[0]))
		tokensUsage(os.Stderr)
		return fmt.Errorf("tokens: unknown subcommand %q", args[0])
	}
}

func runTokensCreate(ctx context.Context, service *auth.Service, args []string) error {
	fs := flag.NewFlagSet("tokens create", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	name := fs.String("name", "", "token name")
	root := fs.Bool("root", false, "create root token")
	replace := fs.Bool("replace", false, "replace existing root token")
	scopesCSV := fs.String("scopes", "", "comma-separated scopes")
	sourceFlag := fs.String("source", string(domain.SourceHuman), "token source: human, agent, or system")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *name == "" {
		if *root {
			*name = "root"
		} else {
			return fmt.Errorf("tokens create: --name is required")
		}
	}
	source := domain.Source(strings.TrimSpace(*sourceFlag))
	if !source.Valid() {
		return fmt.Errorf("tokens create: --source must be one of human, agent, system")
	}

	// Root bootstrap intentionally has no actor. Non-root creation requires a
	// valid root bearer in MERISTEM_TOKEN so the token.created event is
	// attributed to the root token.
	var actorPtr *domain.Token
	if !*root {
		secret := os.Getenv("MERISTEM_TOKEN")
		if secret == "" {
			return fmt.Errorf("tokens create: MERISTEM_TOKEN with a root bearer is required")
		}
		tok, err := service.Authenticate(ctx, secret)
		if err != nil {
			return err
		}
		if !tok.IsRoot {
			return auth.ErrRootRequired
		}
		actorPtr = &tok
	}

	result, err := service.CreateToken(ctx, auth.CreateTokenInput{
		Name:    *name,
		IsRoot:  *root,
		Scopes:  splitCSV(*scopesCSV),
		Source:  source,
		Replace: *replace,
		Actor:   actorPtr,
	})
	if err != nil {
		return err
	}
	fmt.Printf("id=%s\nname=%s\nroot=%t\nsource=%s\nsecret=%s\n", result.Token.ID, result.Token.Name, result.Token.IsRoot, result.Token.Source, result.Secret)
	fmt.Fprintln(os.Stderr, "Store the secret now; meristem only stores its hash.")
	return nil
}

func runTokensList(ctx context.Context, service *auth.Service, w io.Writer) error {
	tokens, err := service.List(ctx)
	if err != nil {
		return err
	}
	for _, tok := range tokens {
		status := "active"
		if tok.RevokedAt != nil {
			status = "revoked"
		}
		fmt.Fprintf(w, "%s\t%s\troot=%t\tsource=%s\t%s\tcreated=%s\n", tok.ID, tok.Name, tok.IsRoot, tok.Source, status, tok.CreatedAt.Format(time.RFC3339))
	}
	return nil
}

func runTokensRevoke(ctx context.Context, service *auth.Service, args []string) error {
	fs := flag.NewFlagSet("tokens revoke", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	idText := fs.String("id", "", "token id")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *idText == "" {
		return fmt.Errorf("tokens revoke: --id is required")
	}
	id, err := uuid.Parse(*idText)
	if err != nil {
		return err
	}
	secret := os.Getenv("MERISTEM_TOKEN")
	if secret == "" {
		return fmt.Errorf("tokens revoke: MERISTEM_TOKEN is required")
	}
	actor, err := service.Authenticate(ctx, secret)
	if err != nil {
		return err
	}
	if err := service.Revoke(ctx, id, actor); err != nil {
		if errors.Is(err, auth.ErrTokenNotFound) {
			return fmt.Errorf("tokens revoke: no such token %s", id)
		}
		return err
	}
	fmt.Printf("revoked=%s\n", id)
	return nil
}

func splitCSV(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func tokensUsage(w io.Writer) {
	fmt.Fprint(w, `usage:
  meristem tokens create --root [--replace] [--name root] [--source human]
  MERISTEM_TOKEN=mrs_... meristem tokens create --name iphone [--source human] [--scopes a,b]
  MERISTEM_TOKEN=mrs_... meristem tokens create --name cursor [--source agent] [--scopes a,b]
  meristem tokens list
  MERISTEM_TOKEN=mrs_... meristem tokens revoke --id <uuid>
`)
}
