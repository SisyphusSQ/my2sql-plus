package loader

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/SisyphusSQ/my2sql/internal/config"
	"github.com/SisyphusSQ/my2sql/internal/log"
	"github.com/SisyphusSQ/my2sql/internal/models"
	"github.com/SisyphusSQ/my2sql/internal/utils"
	"github.com/SisyphusSQ/my2sql/internal/utils/timeutil"
	"github.com/SisyphusSQ/my2sql/internal/vars"
)

type StatsLoad struct {
	sync.Mutex
	ctx context.Context

	absTables  map[string]struct{}
	lastBinlog string

	bigTrxRows  int
	longTrxSecs int
	interval    *time.Ticker

	trxFile     *os.File
	statsFile   *os.File
	trxWriter   *bufio.Writer
	statsWriter *bufio.Writer

	trx       *models.TrxInfo
	stats     map[string]*models.StatsPrint
	statsChan <-chan *models.BinEventStats
}

func NewStatsLoad(ctx context.Context, c *config.Config, statsChan chan *models.BinEventStats) (*StatsLoad, error) {
	var err error
	s := &StatsLoad{
		ctx: ctx,

		absTables:   make(map[string]struct{}),
		interval:    time.NewTicker(time.Duration(c.PrintInterval)),
		bigTrxRows:  c.BigTrxRowLimit,
		longTrxSecs: c.LongTrxSeconds,

		trx:       new(models.TrxInfo),
		stats:     make(map[string]*models.StatsPrint),
		statsChan: statsChan,
	}

	// -------------- new file --------------
	sf := filepath.Join(c.OutputDir, "binlog_status.txt")
	if s.statsFile, err = os.OpenFile(sf, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644); err != nil {
		log.Logger.Error("failed to open binlog_status.txt, err: %v", err)
		return nil, err
	}
	s.statsWriter = bufio.NewWriter(s.statsFile)
	_, _ = s.statsFile.WriteString(vars.GetStatsHeader(utils.ConvertToSliceAny(vars.StatsHeaderColumn)))

	bf := filepath.Join(c.OutputDir, "biglong_trx.txt")
	if s.trxFile, err = os.OpenFile(bf, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644); err != nil {
		log.Logger.Error("failed to open biglong_trx.txt, err: %v", err)
		return nil, err
	}
	s.trxWriter = bufio.NewWriter(s.trxFile)
	_, _ = s.trxFile.WriteString(vars.GetTrxHeader(utils.ConvertToSliceAny(vars.TrxHeaderColumn)))

	return s, nil
}

func (s *StatsLoad) Start() error {
	log.Logger.Info("start thread to analyze statistics from binlog")
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return nil
		case <-ticker.C:
			_ = s.trxWriter.Flush()
			_ = s.statsWriter.Flush()
		case <-s.interval.C:
			s.writeStats()
		case st, ok := <-s.statsChan:
			if !ok {
				return nil
			}

			s.handleStats(st)
		}
	}
}

func (s *StatsLoad) handleStats(st *models.BinEventStats) {
	if s.lastBinlog != st.Binlog {
		s.writeStats()
	}
	s.lastBinlog = st.Binlog

	if st.QueryType == "query" {
		sql := strings.ToLower(st.QuerySQL)

		// trx cannot spreads in different binlogs
		if sql == "begin" {
			// if reach this code, s.trx must be emtpy
			s.trx = &models.TrxInfo{
				Binlog:     st.Binlog,
				StartPos:   st.StartPos,
				Statements: make(map[string]map[string]int),
			}
		} else if utils.EqualsAny(sql, "commit", "rollback") {
			// the rows event may be skipped by --databases --tables
			if s.trx.StartTime > 0 {
				s.trx.StopPos = st.StopPos
				s.trx.StopTime = int64(st.Timestamp)

				if s.trx.RowCnt >= s.bigTrxRows || s.trx.Duration >= s.longTrxSecs {
					s.writeTrx()
				}

				// don't forget to renew trxInfo
				s.trx = new(models.TrxInfo)
			}
		}
		return
	}

	// st.QueryType != "query"
	// if app starts in the middle of trx
	if s.trx.Binlog == "" {
		s.trx.Binlog = st.Binlog
		s.trx.StartPos = st.StartPos
		s.trx.Statements = make(map[string]map[string]int)
	}

	if s.trx.StartTime == 0 {
		s.trx.StartTime = int64(st.Timestamp)
	}
	s.trx.RowCnt += int(st.RowCnt)

	absTable := utils.GetAbsTableName(st.Database, st.Table)
	if _, ok := s.trx.Statements[absTable]; !ok {
		s.trx.Statements[absTable] = map[string]int{"insert": 0, "update": 0, "delete": 0}
	}
	s.trx.Statements[absTable][st.QueryType] += int(st.RowCnt)

	// collect stats
	s.collectStats(absTable, st)
}

func (s *StatsLoad) collectStats(t string, st *models.BinEventStats) {
	if _, ok := s.stats[t]; !ok {
		s.stats[t] = &models.StatsPrint{
			StartTime: int64(st.Timestamp),
			StartPos:  st.StartPos,
			Database:  st.Database,
			Table:     st.Table,
			Inserts:   0,
			Updates:   0,
			Deletes:   0,
		}
	}

	switch st.QueryType {
	case "insert":
		s.stats[t].Inserts += int(st.RowCnt)
	case "update":
		s.stats[t].Updates += int(st.RowCnt)
	case "delete":
		s.stats[t].Deletes += int(st.RowCnt)
	}
	s.stats[t].StopTime = int64(st.Timestamp)
	s.stats[t].StopPos = st.StopPos
}

func (s *StatsLoad) writeStats() {
	s.Lock()
	defer s.Unlock()
	for _, st := range s.stats {
		//[binlog, start_time, stop_time, start_pos, stop_pos, inserts, updates, deletes, database, table]
		_, _ = s.statsWriter.WriteString(fmt.Sprintf("%-17s %-19s %-19s %-10d %-10d %-8d %-8d %-8d %-15s %-20s\n",
			s.lastBinlog, timeutil.UnixTsToCSTLayout(st.StartTime), timeutil.UnixTsToCSTLayout(st.StopTime),
			st.StartPos, st.StopPos, st.Inserts, st.Updates, st.Deletes, st.Database, st.Table))
	}
	s.stats = make(map[string]*models.StatsPrint)
}

func (s *StatsLoad) writeTrx() {
	ss := make([]string, 0, len(s.trx.Statements))
	for absTable, info := range s.trx.Statements {
		ss = append(ss, fmt.Sprintf("%s(inserts=%d, updates=%d, deletes=%d)", absTable, info["insert"], info["update"], info["delete"]))
	}

	//{"binlog", "start_time", "stop_time", "start_pos", "stop_pos", "rows", "duration", "tables"}
	_, _ = s.trxWriter.WriteString(fmt.Sprintf("%-17s %-19s %-19s %-10d %-10d %-8d %-10d %s\n", s.lastBinlog,
		timeutil.UnixTsToCSTLayout(s.trx.StartTime), timeutil.UnixTsToCSTLayout(s.trx.StopTime),
		s.trx.StartPos, s.trx.StopPos, s.trx.RowCnt, s.trx.Duration,
		fmt.Sprintf("[%s]", strings.Join(ss, " "))),
	)
}

func (s *StatsLoad) LastBinlog() string {
	return s.lastBinlog
}

func (s *StatsLoad) Stop() {
	s.interval.Stop()

	// ---------- close file ----------
	_ = s.trxWriter.Flush()
	_ = s.statsWriter.Flush()

	_ = s.trxFile.Close()
	_ = s.statsFile.Close()

	log.Logger.Info("exit thread to analyze statistics from binlog")
}
