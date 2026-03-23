package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/SisyphusSQ/my2sql/internal/vars"
)

func TestParseConfig_AssignAndIgnoreFlags(t *testing.T) {
	t.Run("tables only sets assign table mode", func(t *testing.T) {
		cfg := newTestConfig(t)
		cfg.ParseConfig("", "orders", "", "", "", "", "", false)

		if !cfg.IsAssign {
			t.Fatalf("expected IsAssign to be true")
		}
		if cfg.AssignDB {
			t.Fatalf("expected AssignDB to be false when only tables are provided")
		}
		if !cfg.AssignTB {
			t.Fatalf("expected AssignTB to be true when only tables are provided")
		}
		if !cfg.DBTBExist("shop", "orders", "assign") {
			t.Fatalf("expected table filter to match provided table")
		}
		if cfg.DBTBExist("shop", "payments", "assign") {
			t.Fatalf("expected unrelated table to be excluded")
		}
	})

	t.Run("ignore database only works without ignore table", func(t *testing.T) {
		cfg := newTestConfig(t)
		cfg.ParseConfig("", "", "analytics", "", "", "", "", false)

		if !cfg.IsIgnore {
			t.Fatalf("expected IsIgnore to be true")
		}
		if !cfg.IgnoreDB {
			t.Fatalf("expected IgnoreDB to be true")
		}
		if cfg.IgnoreTB {
			t.Fatalf("expected IgnoreTB to be false")
		}
		if !cfg.DBTBExist("analytics", "events", "ignore") {
			t.Fatalf("expected ignore database rule to match")
		}
		if cfg.DBTBExist("core", "events", "ignore") {
			t.Fatalf("expected other databases to remain included")
		}
	})

	t.Run("ignore table only works without ignore database", func(t *testing.T) {
		cfg := newTestConfig(t)
		cfg.ParseConfig("", "", "", "events", "", "", "", false)

		if !cfg.IsIgnore {
			t.Fatalf("expected IsIgnore to be true")
		}
		if cfg.IgnoreDB {
			t.Fatalf("expected IgnoreDB to be false")
		}
		if !cfg.IgnoreTB {
			t.Fatalf("expected IgnoreTB to be true")
		}
		if !cfg.DBTBExist("analytics", "events", "ignore") {
			t.Fatalf("expected ignore table rule to match")
		}
		if cfg.DBTBExist("analytics", "orders", "ignore") {
			t.Fatalf("expected other tables to remain included")
		}
	})

	t.Run("file mode derives start file from local binlog file", func(t *testing.T) {
		cfg := newTestConfig(t)
		cfg.Mode = "file"

		binlogPath := filepath.Join(t.TempDir(), "mysql-bin.000001")
		if err := os.WriteFile(binlogPath, []byte("binlog"), 0644); err != nil {
			t.Fatalf("unexpected write error: %v", err)
		}

		cfg.LocalBinFile = binlogPath
		cfg.ParseConfig("", "", "", "", "", "", "", false)

		if cfg.GivenBinlogFile != binlogPath {
			t.Fatalf("expected GivenBinlogFile to equal LocalBinFile, got %s", cfg.GivenBinlogFile)
		}
		if cfg.StartFile != "mysql-bin.000001" {
			t.Fatalf("expected StartFile to default to basename, got %s", cfg.StartFile)
		}
		if cfg.BinlogDir != filepath.Dir(binlogPath) {
			t.Fatalf("expected BinlogDir to equal file directory, got %s", cfg.BinlogDir)
		}
	})
}

func newTestConfig(t *testing.T) *Config {
	t.Helper()

	return &Config{
		Mode:               "repl",
		WorkType:           "2sql",
		MySQLType:          "mysql",
		BinlogTimeLocation: "Local",
		OutputDir:          t.TempDir(),
		PrintInterval:      vars.GetDefaultValueOfRange("PrintInterval"),
		BigTrxRowLimit:     vars.GetDefaultValueOfRange("BigTrxRowLimit"),
		LongTrxSeconds:     vars.GetDefaultValueOfRange("LongTrxSeconds"),
		Threads:            vars.GetDefaultValueOfRange("Threads"),
	}
}
