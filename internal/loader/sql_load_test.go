package loader

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SisyphusSQ/my2sql/internal/config"
	"github.com/SisyphusSQ/my2sql/internal/models"
)

func TestHandleSQLPerTable_UsesTableSpecificWriter(t *testing.T) {
	var wg sync.WaitGroup
	cfg := &config.Config{
		OutputDir:    t.TempDir(),
		FilePerTable: true,
		WorkType:     "2sql",
	}

	loader := NewSQLLoader(&wg, context.Background(), cfg, fdFilePrefix, make(chan *models.ResultSQL))
	first := &models.ResultSQL{
		SQLs: []string{"INSERT INTO orders VALUES (1)"},
		SQLInfo: models.ExtraInfo{
			Binlog: "mysql-bin.000001",
			Schema: "shop",
			Table:  "orders",
		},
	}
	second := &models.ResultSQL{
		SQLs: []string{"INSERT INTO payments VALUES (2)"},
		SQLInfo: models.ExtraInfo{
			Binlog: "mysql-bin.000001",
			Schema: "shop",
			Table:  "payments",
		},
	}

	if err := loader.handleSQLPerTable(first); err != nil {
		t.Fatalf("unexpected error writing first table sql: %v", err)
	}
	if err := loader.handleSQLPerTable(second); err != nil {
		t.Fatalf("unexpected error writing second table sql: %v", err)
	}

	for fileName, writer := range loader.writerMap {
		if err := writer.Flush(); err != nil {
			t.Fatalf("unexpected flush error for %s: %v", fileName, err)
		}
		if err := loader.fileMap[fileName].Close(); err != nil {
			t.Fatalf("unexpected close error for %s: %v", fileName, err)
		}
	}

	orderFile := filepath.Join(cfg.OutputDir, "shop.orders.forward.1.sql")
	paymentFile := filepath.Join(cfg.OutputDir, "shop.payments.forward.1.sql")

	orderContent, err := os.ReadFile(orderFile)
	if err != nil {
		t.Fatalf("unexpected read error for orders file: %v", err)
	}
	paymentContent, err := os.ReadFile(paymentFile)
	if err != nil {
		t.Fatalf("unexpected read error for payments file: %v", err)
	}

	if !strings.Contains(string(orderContent), "orders") {
		t.Fatalf("expected orders sql in orders file, got %s", string(orderContent))
	}
	if strings.Contains(string(orderContent), "payments") {
		t.Fatalf("expected orders file to exclude payments sql, got %s", string(orderContent))
	}
	if !strings.Contains(string(paymentContent), "payments") {
		t.Fatalf("expected payments sql in payments file, got %s", string(paymentContent))
	}
}

func TestStatsLoader_HandleStatsCalculatesDuration(t *testing.T) {
	var trxOut bytes.Buffer
	statsOut := io.Discard

	loader := &StatsLoader{
		interval:    time.NewTicker(time.Hour),
		bigTrxRows:  100,
		longTrxSecs: 1,
		trxWriter:   bufio.NewWriter(&trxOut),
		statsWriter: bufio.NewWriter(statsOut),
		trx:         new(models.TrxInfo),
		stats:       make(map[string]*models.StatsPrint),
	}
	defer loader.interval.Stop()

	loader.handleStats(&models.BinEventStats{
		Timestamp: 100,
		Binlog:    "mysql-bin.000001",
		StartPos:  4,
		StopPos:   120,
		QueryType: "query",
		QuerySQL:  "BEGIN",
	})
	loader.handleStats(&models.BinEventStats{
		Timestamp: 101,
		Binlog:    "mysql-bin.000001",
		StartPos:  120,
		StopPos:   180,
		Database:  "shop",
		Table:     "orders",
		QueryType: "update",
		RowCnt:    2,
	})
	loader.handleStats(&models.BinEventStats{
		Timestamp: 103,
		Binlog:    "mysql-bin.000001",
		StartPos:  180,
		StopPos:   220,
		QueryType: "query",
		QuerySQL:  "COMMIT",
	})

	if err := loader.trxWriter.Flush(); err != nil {
		t.Fatalf("unexpected flush error: %v", err)
	}
	if !strings.Contains(trxOut.String(), " 2 ") {
		t.Fatalf("expected trx duration to be written as 2 seconds, got %s", trxOut.String())
	}
}

func TestStatsLoader_WriteSummaryOutputsScreenAndFile(t *testing.T) {
	summaryFile, err := os.CreateTemp(t.TempDir(), "summary-*.txt")
	if err != nil {
		t.Fatalf("unexpected temp file error: %v", err)
	}
	defer summaryFile.Close()

	loader := &StatsLoader{
		interval:       time.NewTicker(time.Hour),
		trxWriter:      bufio.NewWriter(io.Discard),
		statsWriter:    bufio.NewWriter(io.Discard),
		trx:            new(models.TrxInfo),
		stats:          make(map[string]*models.StatsPrint),
		summaryEnabled: true,
		summaryPath:    summaryFile.Name(),
		summary:        make(map[string]*rollbackSummaryEntry),
		summaryFile:    summaryFile,
		summaryW:       bufio.NewWriter(summaryFile),
	}
	defer loader.interval.Stop()

	loader.handleStats(&models.BinEventStats{
		Timestamp: 10,
		Binlog:    "mysql-bin.000001",
		StartPos:  4,
		StopPos:   40,
		Database:  "shop",
		Table:     "orders",
		QueryType: "insert",
		RowCnt:    3,
	})
	loader.handleStats(&models.BinEventStats{
		Timestamp: 11,
		Binlog:    "mysql-bin.000001",
		StartPos:  40,
		StopPos:   80,
		Database:  "shop",
		Table:     "orders",
		QueryType: "update",
		RowCnt:    2,
	})

	stdout := os.Stdout
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("unexpected pipe error: %v", err)
	}
	os.Stdout = writer

	loader.writeSummary()

	_ = writer.Close()
	os.Stdout = stdout

	screenOutput, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("unexpected stdout read error: %v", err)
	}
	fileOutput, err := os.ReadFile(summaryFile.Name())
	if err != nil {
		t.Fatalf("unexpected summary file read error: %v", err)
	}

	for _, output := range []string{string(screenOutput), string(fileOutput)} {
		if !strings.Contains(output, "database") || !strings.Contains(output, "rollback_sql_type") {
			t.Fatalf("expected summary header in output, got %s", output)
		}
		if !strings.Contains(output, "shop") || !strings.Contains(output, "orders") {
			t.Fatalf("expected table identity in output, got %s", output)
		}
		if !strings.Contains(output, "insert") || !strings.Contains(output, "delete") {
			t.Fatalf("expected insert->delete mapping in output, got %s", output)
		}
		if !strings.Contains(output, "update") {
			t.Fatalf("expected update mapping in output, got %s", output)
		}
	}
}
