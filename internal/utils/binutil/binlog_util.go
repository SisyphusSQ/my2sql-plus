package binutil

import "github.com/go-mysql-org/go-mysql/replication"

func GetInfoFromBinevent(ev *replication.BinlogEvent) (string, string, string, string, uint32) {
	var (
		db, tb, sql, sqlType string
		rowCnt               uint32
	)

	switch ev.Header.EventType {
	case replication.WRITE_ROWS_EVENTv1,
		replication.WRITE_ROWS_EVENTv2:

		wrEvent := ev.Event.(*replication.RowsEvent)
		db = string(wrEvent.Table.Schema)
		tb = string(wrEvent.Table.Table)
		sqlType = "insert"
		rowCnt = uint32(len(wrEvent.Rows))

	case replication.UPDATE_ROWS_EVENTv1,
		replication.UPDATE_ROWS_EVENTv2:

		wrEvent := ev.Event.(*replication.RowsEvent)
		db = string(wrEvent.Table.Schema)
		tb = string(wrEvent.Table.Table)
		sqlType = "update"
		rowCnt = uint32(len(wrEvent.Rows)) / 2

	case replication.DELETE_ROWS_EVENTv1,
		replication.DELETE_ROWS_EVENTv2:

		//replication.XID_EVENT,
		//replication.TABLE_MAP_EVENT:

		wrEvent := ev.Event.(*replication.RowsEvent)
		db = string(wrEvent.Table.Schema)
		tb = string(wrEvent.Table.Table)
		sqlType = "delete"
		rowCnt = uint32(len(wrEvent.Rows))

	case replication.QUERY_EVENT:
		queryEvent := ev.Event.(*replication.QueryEvent)
		db = string(queryEvent.Schema)
		sql = string(queryEvent.Query)
		sqlType = "query"

	case replication.MARIADB_GTID_EVENT:
		// For global transaction ID, used to start a new transaction event group, instead of the old BEGIN query event, and also to mark stand-alone (ddl).
		//https://mariadb.com/kb/en/library/gtid_event/
		sql = "begin"
		sqlType = "query"

	case replication.XID_EVENT:
		// XID_EVENT represents commit。rollback transaction not in binlog
		sql = "commit"
		sqlType = "query"
	default:
		// do nothing
	}
	return db, tb, sqlType, sql, rowCnt
}
