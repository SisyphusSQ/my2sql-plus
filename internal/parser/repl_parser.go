package parser

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/SisyphusSQ/my2sql/internal/config"
	"github.com/SisyphusSQ/my2sql/internal/log"
	"github.com/SisyphusSQ/my2sql/internal/models"
	"github.com/SisyphusSQ/my2sql/internal/utils"
	"github.com/SisyphusSQ/my2sql/internal/utils/binutil"
	"github.com/SisyphusSQ/my2sql/internal/vars"
)

type ReplParser struct {
	ctx context.Context

	binlog       string
	eventTimeout time.Duration
	startPos     mysql.Position
	config       *config.Config
	fromDB       *sql.DB
	syncer       *replication.BinlogSyncer

	eventChan chan<- *models.MyBinEvent
	statChan  chan<- *models.BinEventStats
}

func NewReplParser(ctx context.Context, c *config.Config,
	eventChan chan *models.MyBinEvent,
	statChan chan *models.BinEventStats) (*ReplParser, error) {
	var err error
	replCfg := replication.BinlogSyncerConfig{
		ServerID:                uint32(c.ServerId),
		Flavor:                  c.MySQLType,
		Host:                    c.Host,
		Port:                    uint16(c.Port),
		User:                    c.User,
		Password:                c.Passwd,
		Charset:                 "utf8",
		SemiSyncEnabled:         false,
		TimestampStringLocation: c.GTimeLocation,
		ParseTime:               false, //do not parse mysql datetime/time column into go time structure, take it as string
		UseDecimal:              false, // sqlbuilder not support decimal type
	}

	r := &ReplParser{
		ctx:       ctx,
		config:    c,
		binlog:    c.StartFile,
		syncer:    replication.NewBinlogSyncer(replCfg),
		eventChan: eventChan,
		statChan:  statChan,
		startPos:  mysql.Position{Name: c.StartFile, Pos: uint32(c.StartPos)},
	}

	// todo if loc isn't default?
	dsn := fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/?autocommit=true&charset=utf8mb4,utf8,latin1&loc=Local&parseTime=true",
		c.User, c.Passwd, c.Host, c.Port)
	r.fromDB, err = utils.CreateMysqlConn(dsn)
	if err != nil {
		log.Logger.Error("error creating mysql connection: %v", err)
		return nil, err
	}

	return r, nil
}

func (r *ReplParser) Start() error {
	defer r.Stop()

	var (
		err error
		ev  *replication.BinlogEvent

		binEventIdx, trxIdx uint64
		trxStatus           int

		db, tb, text      string
		sqlLower, sqlType string
		rowCnt, tbMapPos  uint32
	)

	log.Logger.Info("starting to get binlog from mysql")
	replStreamer, err := r.syncer.StartSync(r.startPos)
	if err != nil {
		log.Logger.Error("error replication from master, err: %v", err)
		return err
	}

	for {
		ctx, cancel := context.WithTimeout(context.Background(), r.eventTimeout)
		ev, err = replStreamer.GetEvent(ctx)
		cancel()

		if err != nil {
			if errors.Is(err, context.Canceled) {
				log.Logger.Error("ready to quit! [%v]", err)
			} else if errors.Is(err, context.DeadlineExceeded) {
				log.Logger.Error("replStreamer get event deadline exceeded.")
			} else {
				log.Logger.Error("error to get binlog event %v", err)
			}
			return err
		}

		if ev.Header.EventType == replication.TABLE_MAP_EVENT {
			// avoid mysqlbing mask the row event as unknown table row event
			tbMapPos = ev.Header.LogPos - ev.Header.EventSize
		}
		// we don't need raw data
		ev.RawData = []byte{}

		oneEvent := models.NewMyBinEvent(r.binlog, ev.Header.LogPos, tbMapPos)
		state := oneEvent.CheckBinEvent(r.config, ev, &r.binlog)
		if state == vars.ReContinue || state == vars.ReFileEnd {
			continue
		} else if state == vars.ReBreak {
			return nil
		}

		db, tb, sqlType, text, rowCnt = binutil.GetInfoFromBinevent(ev)
		if sqlType == "query" {
			sqlLower = strings.ToLower(text)
			if sqlLower == "begin" {
				trxStatus = vars.TrxBegin
				trxIdx++
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

		if r.config.WorkType != "stats" && oneEvent.IfRowsEvent {
			binEventIdx++
			oneEvent.EventIdx = binEventIdx
			oneEvent.SQLType = sqlType
			oneEvent.Timestamp = ev.Header.Timestamp
			oneEvent.TrxIndex = trxIdx
			oneEvent.TrxStatus = trxStatus
			r.eventChan <- oneEvent
		}

		// output analysis result whatever the WorkType is
		if sqlType != "" {
			r.statChan <- &models.BinEventStats{
				Timestamp: ev.Header.Timestamp,
				Binlog:    r.binlog,
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
		case <-ctx.Done():
			return nil
		default:
			// do nothing
		}
	}
}

func (r *ReplParser) Binlog() string {
	return r.binlog
}

func (r *ReplParser) Stop() {
	if r.fromDB != nil {
		_ = r.fromDB.Close()
	}

	if r.syncer != nil {
		r.syncer.Close()
	}
	log.Logger.Info("finished getting binlog from mysql")
}
