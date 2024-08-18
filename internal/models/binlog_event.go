package models

import (
	"github.com/SisyphusSQ/my2sql/internal/config"
	"github.com/SisyphusSQ/my2sql/internal/log"
	"github.com/SisyphusSQ/my2sql/internal/vars"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

const (
	insert = "insert"
	update = "update"
	delete = "delete"
)

type BinEventStats struct {
	Timestamp     uint32
	Binlog        string
	StartPos      uint32
	StopPos       uint32
	Database      string
	Table         string
	QueryType     string // query, insert, update, delete
	RowCnt        uint32
	QuerySQL      string   // for type = query, insert, update, delete
	ParsedSqlInfo *SQLInfo // for ddl
}

type StatsPrint struct {
	StartTime int64
	StopTime  int64
	StartPos  uint32
	StopPos   uint32
	Database  string
	Table     string
	Inserts   int
	Updates   int
	Deletes   int
}

type TrxInfo struct {
	StartTime  int64
	StopTime   int64
	Binlog     string
	StartPos   uint32
	StopPos    uint32
	RowCnt     int                       // total row count for all statement
	Duration   int                       // how long the trx lasts
	Statements map[string]map[string]int // rowCnt for each type statement: insert, update, delete. {db1.tb1:{insert:0, update:2, delete:10}}
}

type MyBinEvent struct {
	MyPos       mysql.Position //m is the end position
	EventIdx    uint64
	BinEvent    *replication.RowsEvent
	StartPos    uint32 // m is the start position
	IfRowsEvent bool
	SQLType     string // insert, update, delete
	Timestamp   uint32
	TrxIndex    uint64
	TrxStatus   int      // 0:begin, 1: commit, 2: rollback, -1: in_progress
	QuerySQL    *SQLInfo // for ddl and binlog which is not row format
	OrgSQL      string   // for ddl and binlog which is not row format

	isStarted bool
}

func NewMyBinEvent(binlog string, pos, startPos uint32) *MyBinEvent {
	return &MyBinEvent{
		MyPos: mysql.Position{
			Name: binlog,
			Pos:  pos,
		},
		StartPos: startPos,
	}
}

func (m *MyBinEvent) CheckBinEvent(c *config.Config, ev *replication.BinlogEvent, currentBinlog *string) int {
	// ----------- check if rotate -----------
	if ev.Header.EventType == replication.ROTATE_EVENT {
		rotateEvent := ev.Event.(*replication.RotateEvent)
		*currentBinlog = string(rotateEvent.NextLogName)
		m.IfRowsEvent = false
		return vars.ReFileEnd
	}

	// ----------- check whether to start -----------
	if c.IfSetStartFilePos {
		cmpRe := m.MyPos.Compare(c.StartFilePos)
		if cmpRe == -1 {
			return vars.ReContinue
		}
	}

	if c.IfSetStartDateTime {
		if ev.Header.Timestamp < c.StartDatetime {
			return vars.ReContinue
		}
	}

	// ----------- check whether to stop -----------
	if c.IfSetStopFilePos {
		cmpRe := m.MyPos.Compare(c.StopFilePos)
		if cmpRe >= 0 {
			log.Logger.Info("stop to get event. StopFilePos set. currentBinlog %s StopFilePos %s", m.MyPos.String(), c.StopFilePos.String())
			return vars.ReBreak
		}
	}

	if c.IfSetStopDateTime {
		if ev.Header.Timestamp >= c.StopDatetime {
			log.Logger.Info("stop to get event. StopDateTime set. current event Timestamp %d Stop DateTime Timestamp %d", ev.Header.Timestamp, c.StopDatetime)
			return vars.ReBreak
		}
	}

	// ----------- starting to process -----------
	if ev.Header.EventType == replication.WRITE_ROWS_EVENTv1 || ev.Header.EventType == replication.WRITE_ROWS_EVENTv2 {
		if _, ok := c.FilterSQLMap[insert]; !ok {
			return vars.ReContinue
		}
		goto BinEventCheck
	}

	if ev.Header.EventType == replication.UPDATE_ROWS_EVENTv1 || ev.Header.EventType == replication.UPDATE_ROWS_EVENTv2 {
		if _, ok := c.FilterSQLMap[update]; !ok {
			return vars.ReContinue
		}
		goto BinEventCheck
	}

	if ev.Header.EventType == replication.DELETE_ROWS_EVENTv1 || ev.Header.EventType == replication.DELETE_ROWS_EVENTv2 {
		if _, ok := c.FilterSQLMap[delete]; !ok {
			return vars.ReContinue
		}
		goto BinEventCheck
	}

BinEventCheck:
	switch ev.Header.EventType {
	case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2, replication.UPDATE_ROWS_EVENTv1,
		replication.UPDATE_ROWS_EVENTv2, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:

		wrEvent := ev.Event.(*replication.RowsEvent)
		db := string(wrEvent.Table.Schema)
		tb := string(wrEvent.Table.Table)

		isAssign := c.IsAssign && !(c.IsAssign && c.DBTBExist(db, tb, "assign")) // check if assign db or tb
		isIgnore := c.IsIgnore && c.DBTBExist(db, tb, "ignore")                  // check if ignore db or tb
		if isAssign || isIgnore {
			return vars.ReContinue
		}

		m.BinEvent = wrEvent
		m.IfRowsEvent = true
	case replication.QUERY_EVENT:
		m.IfRowsEvent = false

	case replication.XID_EVENT:
		m.IfRowsEvent = false

	case replication.MARIADB_GTID_EVENT:
		m.IfRowsEvent = false

	default:
		m.IfRowsEvent = false
		return vars.ReContinue
	}
	return vars.ReProcess
}
