package loader

import (
	"context"
	"os"
	"os/exec"
	"sync"
	"testing"

	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/SisyphusSQ/my2sql/internal/config"
	"github.com/SisyphusSQ/my2sql/internal/extractor"
	"github.com/SisyphusSQ/my2sql/internal/models"
	"github.com/SisyphusSQ/my2sql/internal/vars"
)

func TestFlashbackLoader_GeneratesParseableBinlogFromSample(t *testing.T) {
	const sampleBinlog = "/Users/suqing/Coding/c/03_iqiyi/myflash/testbinlog/haha.000041"

	if _, err := os.Stat(sampleBinlog); err != nil {
		t.Skipf("sample binlog not available: %v", err)
	}

	cfg := &config.Config{
		Mode:                "file",
		WorkType:            "rollback",
		MySQLType:           "mysql",
		BinlogTimeLocation:  "Local",
		OutputDir:           t.TempDir(),
		PrintInterval:       vars.GetDefaultValueOfRange("PrintInterval"),
		BigTrxRowLimit:      vars.GetDefaultValueOfRange("BigTrxRowLimit"),
		LongTrxSeconds:      vars.GetDefaultValueOfRange("LongTrxSeconds"),
		Threads:             1,
		LocalBinFile:        sampleBinlog,
		FlashbackBinlog:     true,
		FlashbackBinlogBase: "sample-flashback",
	}
	cfg.ParseConfig("", "", "", "", "", "", "", false)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		loadWg    sync.WaitGroup
		extractWg sync.WaitGroup
		eventChan = make(chan *models.MyBinEvent, 128)
		statsChan = make(chan *models.BinEventStats, 128)
		rawChan   = make(chan *models.FlashbackEvent, 128)
	)

	go drainMyBinEvents(ctx, eventChan)
	go drainStats(ctx, statsChan)

	loader := NewFlashbackLoader(&loadWg, ctx, cfg, rawChan)
	loaderErrCh := make(chan error, 1)
	go func() {
		loaderErrCh <- loader.Start()
		loader.Stop()
	}()

	fileExtractor := extractor.NewFileExtract(&extractWg, ctx, cfg, eventChan, statsChan, rawChan)
	extractErr := fileExtractor.Start()
	fileExtractor.Stop()

	close(rawChan)
	cancel()

	loadErr := <-loaderErrCh
	loadWg.Wait()
	extractWg.Wait()

	if extractErr != nil {
		t.Fatalf("unexpected flashback extractor error: %v", extractErr)
	}
	if loadErr != nil {
		t.Fatalf("unexpected flashback loader error: %v", loadErr)
	}

	parser := replication.NewBinlogParser()
	count := 0
	if err := parser.ParseFile(cfg.FlashbackBinlogPath+vars.FlashbackBinlogSuffix, 0, func(ev *replication.BinlogEvent) error {
		count++
		return nil
	}); err != nil {
		t.Fatalf("expected generated flashback binlog to be parseable, got %v", err)
	}
	if count == 0 {
		t.Fatalf("expected generated flashback binlog to contain events")
	}

	if _, err := exec.LookPath("mysqlbinlog"); err == nil {
		cmd := exec.Command("mysqlbinlog", cfg.FlashbackBinlogPath+vars.FlashbackBinlogSuffix)
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("expected mysqlbinlog to read generated flashback file, err=%v output=%s", err, string(output))
		}
	}
}

func drainMyBinEvents(ctx context.Context, eventChan <-chan *models.MyBinEvent) {
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-eventChan:
			if !ok {
				return
			}
		}
	}
}

func drainStats(ctx context.Context, statsChan <-chan *models.BinEventStats) {
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-statsChan:
			if !ok {
				return
			}
		}
	}
}
