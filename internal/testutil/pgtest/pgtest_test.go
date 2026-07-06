package pgtest

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestIntegrationTestsUseSharedHarness(t *testing.T) {
	root := filepath.Clean(filepath.Join("..", "..", ".."))
	for _, dir := range []string{"internal", "cmd"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || !strings.HasSuffix(path, "_integration_test.go") {
				return nil
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			content := string(raw)
			for _, forbidden := range []string{
				"CREATE DATABASE ",
				"DROP DATABASE IF EXISTS",
				"MERISTEM_INTEGRATION",
				"MERISTEM_TEST_DATABASE_URL",
			} {
				if strings.Contains(content, forbidden) {
					t.Errorf("%s contains %q; use internal/testutil/pgtest.NewPool for Postgres integration lifecycle", path, forbidden)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", dir, err)
		}
	}
}
