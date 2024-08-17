package utils

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"

	"github.com/SisyphusSQ/my2sql/internal/log"
)

const (
	PrimaryKey      = "primary"
	UniqueKey       = "unique"
	KeyBinlogPosSep = "/"
	KeyDbTableSep   = "."
	KeyNoneBinlog   = "_"
)

func CreateMysqlConn(dsn string) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		_ = db.Close()
		return nil, err
	}

	return db, nil
}

func GetAbsTableName(schema, table string) string {
	return fmt.Sprintf("%s%s%s", schema, KeyDbTableSep, table)
}

func IsUnsigned(field string) bool {
	return strings.Contains(strings.ToLower(field), "unsigned")
}

func ColumnValue(scanArgs []any, cols []string, colName string) string {
	var c = columnIndex(cols, colName)
	if c == -1 {
		return ""
	}
	return string(*scanArgs[c].(*sql.RawBytes))
}

func ColumnValueInt64(scanArgs []interface{}, slaveCols []string, colName string) (int64, error) {
	var c = columnIndex(slaveCols, colName)
	if c == -1 {
		return 0, nil
	}

	v := string(*scanArgs[c].(*sql.RawBytes))
	i, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, err
	}

	return i, nil
}

func columnIndex(cols []string, colName string) int {
	for idx := range cols {
		if cols[idx] == colName {
			return idx
		}
	}
	return -1
}

func GetFieldType(filed string) string {
	arr := strings.Split(filed, "(")
	if len(arr) < 1 {
		log.Logger.Fatal("get field is null %s", filed)
	}
	return arr[0]
}

func IsPrimary(key string) bool {
	return strings.Contains(strings.ToLower(key), PrimaryKey)
}
