package transformer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	sql "github.com/SisyphusSQ/godropbox/database/sqlbuilder"
	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/SisyphusSQ/my2sql/internal/config"
	"github.com/SisyphusSQ/my2sql/internal/locker"
	"github.com/SisyphusSQ/my2sql/internal/log"
	"github.com/SisyphusSQ/my2sql/internal/models"
	"github.com/SisyphusSQ/my2sql/internal/utils"
	"github.com/SisyphusSQ/my2sql/internal/utils/binutil"
	"github.com/SisyphusSQ/my2sql/internal/utils/sqltypes"
	"github.com/SisyphusSQ/my2sql/internal/utils/timeutil"
)

type Transformer struct {
	wg  *sync.WaitGroup
	ctx context.Context

	binlog    string
	posStr    string
	threadNum int
	trCnt     *atomic.Int64

	isUkFirst  bool
	isRollBack bool
	isFullCols bool
	isPrefixDB bool
	isIgrPri   bool

	curDB       string
	curTb       string
	curAbsTb    string
	curTs       uint32
	curPriIdxes []int
	curUkIdxes  []int
	curColsDef  []sql.NonAliasColumn
	curTbInfo   *models.TblInfo

	ev         *models.MyBinEvent
	tbColsInfo *models.TblColsInfo
	trxLock    *locker.TrxLock

	eventChan <-chan *models.MyBinEvent

	sqlChan  chan *models.ResultSQL
	jsonChan chan *models.ResultSQL
}

func NewTransformer(wg *sync.WaitGroup, ctx context.Context,
	threadNum int,
	trCnt *atomic.Int64,
	c *config.Config,
	tbColsInfo *models.TblColsInfo,
	eventChan chan *models.MyBinEvent,
	sqlChan chan *models.ResultSQL,
	jsonChan chan *models.ResultSQL,
	trxLock *locker.TrxLock) *Transformer {
	t := &Transformer{
		wg:        wg,
		ctx:       ctx,
		threadNum: threadNum,
		trCnt:     trCnt,

		isUkFirst:  c.UseUniqueKeyFirst,
		isRollBack: c.WorkType == "rollback",
		isFullCols: c.FullColumns,
		isPrefixDB: c.SQLTblPrefixDB,
		isIgrPri:   c.IgnorePrimaryKeyForInsert,

		tbColsInfo: tbColsInfo,
		eventChan:  eventChan,
		sqlChan:    sqlChan,
		jsonChan:   jsonChan,

		trxLock: trxLock,
	}

	return t
}

func (t *Transformer) Start() error {
	t.wg.Add(1)
	log.Logger.Info("start thread %d to generate redo/rollback sql", t.threadNum)
	var err error

	for {
		select {
		case <-t.ctx.Done():
			t.trCnt.Add(1)
			return nil
		case ev, ok := <-t.eventChan:
			if !ok {
				t.trCnt.Add(1)
				return nil
			}

			err = t.generate(ev)
			if err != nil {
				return err
			}
		}
	}
}

func (t *Transformer) generate(ev *models.MyBinEvent) error {
	if !ev.IfRowsEvent {
		// todo generate ddl
		return nil
	}

	t.ev = ev
	t.curTs = ev.Timestamp
	t.binlog = ev.MyPos.Name
	t.curDB = string(ev.BinEvent.Table.Schema)
	t.curTb = string(ev.BinEvent.Table.Table)
	t.curAbsTb = utils.GetAbsTableName(t.curDB, t.curTb)
	t.posStr = binutil.GetPosStr(ev.MyPos.Name, ev.StartPos, ev.MyPos.Pos)
	t.curTbInfo = t.tbColsInfo.GetTableInfo(t.curDB, t.curTb) // if error occur, app will panic
	t.handleSpType()                                          // if error occur, app will panic
	t.getUkIndex()

	colCnt := len(ev.BinEvent.Rows[0])
	if colCnt > len(t.curTbInfo.Columns) {
		log.Logger.Fatal("%s column count %d in binlog > in table structure %d, usually means DDL in the middle",
			t.curAbsTb, colCnt, len(t.curTbInfo.Columns))
	}

	// -------------- start to transform --------------
	t.getFieldsExpr(colCnt)
	var (
		sqls        = t.transform2SQL()
		jsonEvents  = t.transform2Json()
		extractInfo = models.ExtraInfo{
			Schema:    t.curDB,
			Table:     t.curTb,
			Binlog:    t.binlog,
			StartPos:  t.ev.StartPos,
			EndPos:    t.ev.MyPos.Pos,
			Datetime:  timeutil.UnixTsToCSTLayout(int64(t.curTs)),
			TrxIndex:  t.ev.TrxIndex,
			TrxStatus: t.ev.TrxStatus,
		}
	)

	// set trx index seq
	for {
		t.trxLock.Lock()
		if t.trxLock.EvIdx() == t.ev.EventIdx {
			res := &models.ResultSQL{SQLs: sqls, Jsons: jsonEvents, SQLInfo: extractInfo}
			t.sqlChan <- res
			t.jsonChan <- res

			t.trxLock.IncrEvIdx()
			t.trxLock.Unlock()
			return nil
		}

		t.trxLock.Unlock()
		time.Sleep(1 * time.Microsecond)
	}
}

func (t *Transformer) CurPos() string {
	return t.posStr
}

func (t *Transformer) Stop() {
	// t.tbColsInfo is shared with multi transformers, so that close it by outer func
	// t.tbColsInfo.Stop()

	t.wg.Done()
	log.Logger.Info(fmt.Sprintf("exit thread %d to generate redo/rollback sql", t.threadNum))
}

func (t *Transformer) getFieldsExpr(colCnt int) {
	var (
		tbMap    = t.ev.BinEvent.Table
		defExprs = make([]sql.NonAliasColumn, 0, colCnt)
	)

	for i := range colCnt {
		defExpr := t.getDataTypeAndSQLCol(t.curTbInfo.Columns[i].FieldName, t.curTbInfo.Columns[i].FieldType,
			tbMap.ColumnType[i], tbMap.ColumnMeta[i])
		defExprs = append(defExprs, defExpr)
	}
	t.curColsDef = defExprs
}

func (t *Transformer) getDataTypeAndSQLCol(colName string, tpDef string, tp byte, meta uint16) sql.NonAliasColumn {
	//get real string type
	if tp == mysql.MYSQL_TYPE_STRING {
		if meta >= 256 {
			b0 := uint8(meta >> 8)
			if b0&0x30 != 0x30 {
				tp = b0 | 0x30
			} else {
				tp = b0
			}
		}
	}

	switch tp {
	case mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_TINY, mysql.MYSQL_TYPE_SHORT,
		mysql.MYSQL_TYPE_INT24, mysql.MYSQL_TYPE_LONGLONG, mysql.MYSQL_TYPE_BIT:
		return sql.IntColumn(colName, sql.NotNullable)
	case mysql.MYSQL_TYPE_NEWDECIMAL, mysql.MYSQL_TYPE_FLOAT, mysql.MYSQL_TYPE_DOUBLE:
		return sql.DoubleColumn(colName, sql.NotNullable)
	case mysql.MYSQL_TYPE_TIMESTAMP, mysql.MYSQL_TYPE_TIMESTAMP2, mysql.MYSQL_TYPE_DATETIME,
		mysql.MYSQL_TYPE_DATETIME2, mysql.MYSQL_TYPE_TIME, mysql.MYSQL_TYPE_TIME2,
		mysql.MYSQL_TYPE_DATE:
		//return "timestamp", sql.DateTimeColumn(colName, sql.NotNullable)
		return sql.StrColumn(colName, sql.UTF8, sql.UTF8CaseInsensitive, sql.NotNullable)
	case mysql.MYSQL_TYPE_YEAR:
		return sql.IntColumn(colName, sql.NotNullable)
	case mysql.MYSQL_TYPE_ENUM, mysql.MYSQL_TYPE_SET:
		return sql.IntColumn(colName, sql.NotNullable)
	case mysql.MYSQL_TYPE_BLOB:
		//text is stored as blob
		if strings.Contains(strings.ToLower(tpDef), "text") {
			return sql.StrColumn(colName, sql.UTF8, sql.UTF8CaseInsensitive, sql.NotNullable)
		}
		return sql.BytesColumn(colName, sql.NotNullable)
	case mysql.MYSQL_TYPE_VARCHAR, mysql.MYSQL_TYPE_VAR_STRING, mysql.MYSQL_TYPE_STRING:
		return sql.StrColumn(colName, sql.UTF8, sql.UTF8CaseInsensitive, sql.NotNullable)
	case mysql.MYSQL_TYPE_JSON:
		//return "json", sql.BytesColumn(colName, sql.NotNullable)
		return sql.StrColumn(colName, sql.UTF8, sql.UTF8CaseInsensitive, sql.NotNullable)
	case mysql.MYSQL_TYPE_GEOMETRY:
		return sql.BytesColumn(colName, sql.NotNullable)

	case mysql.MYSQL_TYPE_NULL:
		fallthrough
	default:
		panic(fmt.Sprintf("unknown column type: %v", tp))
	}
}

func (t *Transformer) handleSpType() {
	for ci, col := range t.curTbInfo.Columns {
		if strings.Contains(col.FieldType, "int") && col.IsUnsigned {
			for ri := range t.ev.BinEvent.Rows {
				t.ev.BinEvent.Rows[ri][ci] = sqltypes.ConvertIntUnsigned(t.ev.BinEvent.Rows[ri][ci], col.FieldType)
			}

			continue
		}

		if strings.Contains(col.FieldType, "text") {
			for ri, _ := range t.ev.BinEvent.Rows {
				if t.ev.BinEvent.Rows[ri][ci] == nil {
					continue
				}

				text, ok := t.ev.BinEvent.Rows[ri][ci].([]byte)
				if !ok {
					log.Logger.Fatal("%s.%s %v []byte  empty %s",
						string(t.ev.BinEvent.Table.Table), col.FieldName, t.ev.BinEvent.Rows[ri][ci], t.posStr)
				}

				t.ev.BinEvent.Rows[ri][ci] = string(text)
			}
		}
	}
}

func (t *Transformer) getUkIndex() {
	var (
		eq       bool
		cols     models.KeyInfo
		tbInfo   = t.curTbInfo
		UkIdxes  = make([]int, 0)
		priIdxes = make([]int, 0)
	)

	// ----------- get unique keys -----------
	if t.isUkFirst && len(tbInfo.UniqueKeys) > 0 {
		cols = tbInfo.UniqueKeys[0]
	} else if len(tbInfo.PrimaryKey) > 0 {
		eq = true
		cols = tbInfo.PrimaryKey
	} else {
		t.curUkIdxes = UkIdxes
		goto pri
	}

	for _, c := range tbInfo.Columns {
		for _, name := range cols {
			if name == c.FieldName {
				UkIdxes = append(UkIdxes, c.Index)
			}
		}
	}
	t.curUkIdxes = UkIdxes

pri:
	// ----------- get primary keys -----------
	if len(tbInfo.PrimaryKey) == 0 {
		t.curPriIdxes = make([]int, 0)
	} else if eq {
		t.curPriIdxes = t.curUkIdxes
	} else {
		for _, c := range tbInfo.Columns {
			for _, name := range tbInfo.PrimaryKey {
				if name == c.FieldName {
					priIdxes = append(priIdxes, c.Index)
				}
			}
		}
		t.curPriIdxes = priIdxes
	}
}

// transform2Json start to transform binlog event to sqls
func (t *Transformer) transform2SQL() []string {
	var (
		sqls    = make([]string, 0)
		sqlType = t.ev.SQLType
	)

	if sqlType == "insert" {
		if t.isRollBack {
			sqls = t.genDelFromEvent()
		} else {
			sqls = t.genInsFromEvent()
		}
	} else if sqlType == "delete" {
		if t.isRollBack {
			sqls = t.genInsFromEvent()
		} else {
			sqls = t.genDelFromEvent()
		}
	} else if sqlType == "update" {
		sqls = t.genUpdFromEvent()
	} else {
		log.Logger.Warn("unsupported query type %s to generate 2sql|rollback sql, it should one of insert|update|delete. %s",
			sqlType, t.posStr)
		return sqls
	}

	return sqls
}

func (t *Transformer) genInsFromEvent() []string {
	var (
		sqlType  string
		schema   = t.curDB
		ifIgrPri = true
		rows     = t.ev.BinEvent.Rows
		rowCnt   = len(rows)
		colsDef  = t.curColsDef
		sqls     = make([]string, 0, rowCnt)
	)

	if !t.isPrefixDB {
		schema = ""
	}

	if t.isRollBack {
		sqlType = "insert_for_delete_rollback"
		ifIgrPri = false
	} else {
		sqlType = "insert"
	}

	if len(t.curPriIdxes) == 0 {
		ifIgrPri = false
	}

	if t.isIgrPri && ifIgrPri {
		d := make([]sql.NonAliasColumn, 0)
		for i := range t.curColsDef {
			if slices.Contains(t.curPriIdxes, i) {
				continue
			}

			d = append(d, t.curColsDef[i])
		}
		colsDef = d
	}

	for _, row := range rows {
		exprs := t.genInsExpress(row, t.isIgrPri && ifIgrPri)
		s, err := sql.NewTable(t.curTb, colsDef...).Insert(colsDef...).Add(exprs...).String(schema)
		if err != nil {
			log.Logger.Fatal(fmt.Sprintf("Fail to generate %s sql for %s %s \n\terror: %v\n\trows data:%v",
				sqlType, t.curAbsTb, t.posStr, err, row))
		}
		sqls = append(sqls, s)
	}
	return sqls
}

func (t *Transformer) genInsExpress(row []any, ifIgnorePrimary bool) []sql.Expression {
	res := make([]sql.Expression, 0)
	for colIdx, colValue := range row {
		if ifIgnorePrimary && slices.Contains(t.curPriIdxes, colIdx) {
			continue
		}

		r := sql.Literal(colValue)
		res = append(res, r)
	}
	return res
}

func (t *Transformer) genUpdFromEvent() []string {
	var (
		sqlType string
		schema  = t.curDB
		rows    = t.ev.BinEvent.Rows
		rowCnt  = len(rows)
		sqls    = make([]string, 0)
	)

	if !t.isPrefixDB {
		schema = ""
	}

	if t.isRollBack {
		sqlType = "update_for_update_rollback"
	} else {
		sqlType = "update"
	}

	for i := 0; i < rowCnt; i += 2 {
		update := sql.NewTable(t.curTb, t.curColsDef...).Update()

		if t.isRollBack {
			update = t.genUpdSetPart(update, rows[i], rows[i+1])
			update.Where(sql.And(t.genEqCond(rows[i+1])...))
		} else {
			update = t.genUpdSetPart(update, rows[i+1], rows[i])
			update.Where(sql.And(t.genEqCond(rows[i])...))
		}

		s, err := update.String(schema)
		if err != nil {
			log.Logger.Fatal("Fail to generate %s sql for %s %s \n\terror: %s\n\trows data:%v\n%v",
				sqlType, t.curAbsTb, t.posStr, err, rows[i], rows[i+1])
		}
		sqls = append(sqls, s)
	}
	return sqls
}

func (t *Transformer) genUpdSetPart(update sql.UpdateStatement, ra []any, rb []any) sql.UpdateStatement {
	for colIdx, colValue := range ra {
		ifUpdate := false

		if !t.isFullCols {
			if slices.Contains([]string{"blob", "json", "geometry"}, t.curTbInfo.Columns[colIdx].FieldName) &&
				!strings.Contains(strings.ToLower(t.curTbInfo.Columns[colIdx].FieldType), "text") {
				a, aOk := colValue.([]byte)
				b, bOk := rb[colIdx].([]byte)
				if aOk && bOk {
					ifUpdate = bytes.Compare(a, b) == 0
				}
			} else {
				ifUpdate = colValue == rb[colIdx]
			}
		} else {
			ifUpdate = true
		}

		if ifUpdate {
			update.Set(t.curColsDef[colIdx], sql.Literal(colValue))
		}
	}
	return update
}

func (t *Transformer) genDelFromEvent() []string {
	var (
		sqlType string
		schema  = t.curDB
		rows    = t.ev.BinEvent.Rows
		rowCnt  = len(rows)
		sqls    = make([]string, 0, rowCnt)
	)

	if !t.isPrefixDB {
		schema = ""
	}

	if t.isRollBack {
		sqlType = "delete_for_insert_rollback"
	} else {
		sqlType = "delete"
	}

	for _, row := range rows {
		whereCond := t.genEqCond(row)
		s, err := sql.NewTable(t.curTb, t.curColsDef...).Delete().Where(sql.And(whereCond...)).String(schema)
		if err != nil {
			log.Logger.Fatal("Fail to generate %s sql for %s %s \n\terror: %s\n\trows data:%v",
				sqlType, t.curAbsTb, t.posStr, err, row)
		}
		sqls = append(sqls, s)
	}
	return sqls
}

func (t *Transformer) genEqCond(row []any) []sql.BoolExpression {
	exps := make([]sql.BoolExpression, 0)
	if !t.isFullCols && len(t.curUkIdxes) > 0 {
		for _, idx := range t.curUkIdxes {
			exp := sql.EqL(t.curColsDef[idx], row[idx])
			exps = append(exps, exp)
		}
		return exps
	}

	for colIdx, colValue := range row {
		exp := sql.EqL(t.curColsDef[colIdx], colValue)
		exps = append(exps, exp)
	}
	return exps
}

// transform2Json start to transform event to json
func (t *Transformer) transform2Json() []string {
	var (
		step       = 1
		rows       = t.ev.BinEvent.Rows
		sqlType    = t.ev.SQLType
		tbInfo     = t.curTbInfo
		jsonEvents = make([]string, 0)
	)

	if sqlType == "update" {
		step = 2
	}

	for i := 0; i < len(rows); i += step {
		event := &models.JsonEvent{
			EventType:  strings.ToUpper(sqlType),
			SchemaName: tbInfo.Database,
			TableName:  tbInfo.Table,
			Timestamp:  t.curTs,
			Position:   t.posStr,
		}

		if sqlType == "insert" {
			event.RowAfter = t.genRowMap(rows[i])
		} else if sqlType == "delete" {
			event.RowBefore = t.genRowMap(rows[i])
		} else if sqlType == "update" {
			event.RowBefore = t.genRowMap(rows[i])
			event.RowAfter = t.genRowMap(rows[i+1])
		}

		data, err := json.Marshal(event)
		if err != nil {
			log.Logger.Error("canalEvent can not be marshaled, value: %v", event)
			continue
		}
		jsonEvents = append(jsonEvents, string(data))
	}
	return jsonEvents
}

func (t *Transformer) genRowMap(rowImages []any) map[string]any {
	if len(t.curTbInfo.Columns) != len(rowImages) {
		log.Logger.Warn("columns len:[%d] and value len:[%d] is not equal", len(t.curTbInfo.Columns), len(rowImages))
		return nil
	}

	rowMap := make(map[string]any)
	for i, rowImage := range rowImages {
		rowMap[t.curTbInfo.Columns[i].FieldName] = rowImage
	}
	return rowMap
}
