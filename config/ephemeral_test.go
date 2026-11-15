package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"codeberg.org/emersion/soju/database"
	"codeberg.org/emersion/soju/msgstore"
)

func writeTempConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "soju.conf")
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatalf("failed to write config: %v", err)
	}
	return path
}

func TestEphemeralConfig_Load(t *testing.T) {
	t.Run("valid_defaults", func(t *testing.T) {
		path := writeTempConfig(t, `
ephemeral-mode true
`)
		cfg, err := Load(path)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !cfg.EphemeralMode {
			t.Fatal("expected EphemeralMode to be true")
		}
		if cfg.EphemeralGrace != 15*time.Minute {
			t.Fatalf("got EphemeralGrace=%v, want 15m", cfg.EphemeralGrace)
		}
		if cfg.EphemeralHistory != 24*time.Hour {
			t.Fatalf("got EphemeralHistory=%v, want 24h", cfg.EphemeralHistory)
		}
	})

	t.Run("valid_custom_values_with_ephemeral_store", func(t *testing.T) {
		path := writeTempConfig(t, `
db sqlite3 /tmp/test.db
message-store ephemeral
ephemeral-mode true
ephemeral-quit-msg "SIGNAL·DROPPED"
ephemeral-grace 5m
ephemeral-history 30m
`)
		cfg, err := Load(path)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.MsgStore.Driver != msgstore.DriverEphemeral {
			t.Fatalf("got MsgStore.Driver=%q, want %q", cfg.MsgStore.Driver, msgstore.DriverEphemeral)
		}
		if cfg.EphemeralQuitMsg != "SIGNAL·DROPPED" {
			t.Fatalf("got EphemeralQuitMsg=%q, want SIGNAL·DROPPED", cfg.EphemeralQuitMsg)
		}
		if cfg.EphemeralGrace != 5*time.Minute {
			t.Fatalf("got EphemeralGrace=%v, want 5m", cfg.EphemeralGrace)
		}
		if cfg.EphemeralHistory != 30*time.Minute {
			t.Fatalf("got EphemeralHistory=%v, want 30m", cfg.EphemeralHistory)
		}
	})

	t.Run("valid_day_suffix_and_zero", func(t *testing.T) {
		path := writeTempConfig(t, `
ephemeral-mode true
ephemeral-grace 0
ephemeral-history 1d
`)
		cfg, err := Load(path)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.EphemeralGrace != 0 {
			t.Fatalf("got EphemeralGrace=%v, want 0", cfg.EphemeralGrace)
		}
		if cfg.EphemeralHistory != 24*time.Hour {
			t.Fatalf("got EphemeralHistory=%v, want 24h", cfg.EphemeralHistory)
		}
	})

	t.Run("reject_fs_msgstore_with_ephemeral_mode", func(t *testing.T) {
		path := writeTempConfig(t, `
ephemeral-mode true
message-store fs /tmp/logs
`)
		if _, err := Load(path); err == nil {
			t.Fatal("expected error for fs message-store with ephemeral-mode")
		}
	})

	t.Run("reject_memory_msgstore_with_ephemeral_mode", func(t *testing.T) {
		path := writeTempConfig(t, `
ephemeral-mode true
message-store memory
`)
		if _, err := Load(path); err == nil {
			t.Fatal("expected error for memory message-store with ephemeral-mode")
		}
	})

	t.Run("reject_postgres_with_ephemeral_msgstore", func(t *testing.T) {
		path := writeTempConfig(t, `
db postgres "dbname=soju"
message-store ephemeral
`)
		if _, err := Load(path); err == nil {
			t.Fatal("expected error for postgres db with ephemeral message-store")
		}
	})

	t.Run("ephemeral_msgstore_requires_zero_params", func(t *testing.T) {
		path := writeTempConfig(t, `
message-store ephemeral /tmp/foo
`)
		if _, err := Load(path); err == nil {
			t.Fatal("expected error when passing parameter to ephemeral message-store")
		}
	})

	t.Run("default_db_is_sqlite3", func(t *testing.T) {
		path := writeTempConfig(t, `
message-store ephemeral
`)
		cfg, err := Load(path)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.DB.Driver != database.DriverSQLite3 {
			t.Fatalf("got DB.Driver=%q, want sqlite3", cfg.DB.Driver)
		}
	})
}
