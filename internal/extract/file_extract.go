package extract

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/SisyphusSQ/my2sql/internal/config"
	"github.com/SisyphusSQ/my2sql/internal/log"
	"github.com/SisyphusSQ/my2sql/internal/models"
	"github.com/SisyphusSQ/my2sql/internal/utils"
	"github.com/SisyphusSQ/my2sql/internal/utils/binutil"
	"github.com/SisyphusSQ/my2sql/internal/vars"
)

type FileExtract struct {
	ctx context.Context

	binlog      string
	trxIdx      uint64
	binEventIdx uint64
	startPos    mysql.Position
	config      *config.Config
	parser      *replication.BinlogParser

	eventChan chan<- *models.MyBinEvent
	statChan  chan<- *models.BinEventStats
}

func NewFileExtract(ctx context.Context, c *config.Config,
	eventChan chan *models.MyBinEvent,
	statChan chan *models.BinEventStats) *FileExtract {
	f := &FileExtract{
		ctx:       ctx,
		config:    c,
		binlog:    c.StartFile,
		parser:    replication.NewBinlogParser(),
		eventChan: eventChan,
		statChan:  statChan,
		startPos: mysql.Position{
			Name: c.StopFile,
			Pos:  uint32(c.StartPos),
		},
	}
	return f
}

func (f *FileExtract) Start() error {
	defer f.Stop()
	log.Logger.Info("starting to parse binlog from local files")

	var pos int64
	f.binlog, pos = f.getFirstBinlogPosToParse()
	baseName, baseIdx := utils.GetLogNameAndIndex(f.binlog)
	log.Logger.Info("start to parse %s %d", f.binlog, pos)

	for {
		if f.config.IfSetStopFilePos {
			if f.config.StopFilePos.Compare(mysql.Position{Name: filepath.Base(f.binlog), Pos: 4}) < 1 {
				return nil
			}
		}

		// todo now pos in uncorrected
		log.Logger.Info(fmt.Sprintf("start to parse %s %d", f.binlog, pos))
		state, err := f.parsePerFile(f.binlog, pos)
		if err != nil {
			return err
		}

		if state == vars.ReBreak {
			return nil
		} else if state == vars.ReFileEnd {
			// just parse one binlog
			if !f.config.IfSetStopParsPoint && !f.config.IfSetStopDateTime {
				return nil
			}

			f.binlog = filepath.Join(f.config.BinlogDir, utils.GetNextBinlog(baseName, &baseIdx))
			if !utils.IsFile(f.binlog) {
				log.Logger.Warn("%s not exists nor a file", f.binlog)
				return nil
			}

			pos = 4
		} else {
			return errors.New(fmt.Sprintf("this should not happen: return value of MyParseOneBinlog is %d", state))
		}
	}
}

func (f *FileExtract) parsePerFile(fileName string, pos int64) (int, error) {
	var (
		err               error
		db, tb, text      string
		sqlLower, sqlType string
		rowCnt, tbMapPos  uint32
		trxStatus         int
	)

	fn := func(ev *replication.BinlogEvent) error {
		if ev.Header.EventType == replication.TABLE_MAP_EVENT {
			// avoid mysqlbing mask the row event as unknown table row event
			tbMapPos = ev.Header.LogPos - ev.Header.EventSize
		}

		// we don't need raw data
		ev.RawData = []byte{}

		oneEvent := models.NewMyBinEvent(f.binlog, ev.Header.LogPos, tbMapPos)
		state := oneEvent.CheckBinEvent(f.config, ev, &f.binlog)
		if state == vars.ReContinue || state == vars.ReFileEnd {
			return nil
		} else if state == vars.ReBreak {
			// todo change err msg
			return errors.New("break")
		}

		db, tb, sqlType, text, rowCnt = binutil.GetInfoFromBinevent(ev)
		if sqlType == "query" {
			sqlLower = strings.ToLower(text)
			if sqlLower == "begin" {
				trxStatus = vars.TrxBegin
				f.trxIdx++
			} else if sqlLower == "commit" {
				trxStatus = vars.TrxCommit
			} else if sqlLower == "rollback" {
				trxStatus = vars.TrxRollback
			} else if oneEvent.QuerySQL != nil {
				// this condition never become true
				trxStatus = vars.TrxProcess
				rowCnt = 1
			}
		} else {
			trxStatus = vars.TrxProcess
		}

		if f.config.WorkType != "stats" && oneEvent.IfRowsEvent {
			f.binEventIdx++
			oneEvent.EventIdx = f.binEventIdx
			oneEvent.SQLType = sqlType
			oneEvent.Timestamp = ev.Header.Timestamp
			oneEvent.TrxIndex = f.trxIdx
			oneEvent.TrxStatus = trxStatus
			f.eventChan <- oneEvent
		}

		// output analysis result whatever the WorkType is
		if sqlType != "" {
			f.statChan <- &models.BinEventStats{
				Timestamp: ev.Header.Timestamp,
				Binlog:    f.binlog,
				StartPos:  tbMapPos,
				StopPos:   ev.Header.LogPos,
				Database:  db,
				Table:     tb,
				QuerySQL:  text,
				RowCnt:    rowCnt,
				QueryType: sqlType,
			}
		}

		select {
		case <-f.ctx.Done():
			return f.ctx.Err()
		default:
			// do nothing
		}
		return nil
	}

	err = f.parser.ParseFile(fileName, pos, fn)
	if err != nil {
		log.Logger.Error("parse file %s error: %v", fileName, err)
		return vars.ReBreak, err
	}

	return vars.ReFileEnd, nil
}

func (f *FileExtract) getFirstBinlogPosToParse() (string, int64) {
	var binlog string
	var pos int64
	if f.config.StartFile != "" {
		binlog = filepath.Join(f.config.BinlogDir, f.config.StartFile)
	} else {
		binlog = f.config.GivenBinlogFile
	}
	if f.config.StartPos != 0 {
		pos = int64(f.config.StartPos)
	} else {
		pos = 4
	}

	return binlog, pos
}

func (f *FileExtract) Binlog() string {
	return f.binlog
}

func (f *FileExtract) Stop() {
	if f.parser != nil {
		f.parser.Stop()
	}

	log.Logger.Info("finished parsing binlog from local files")
}
