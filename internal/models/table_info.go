package models

import (
	"database/sql"
	"fmt"
	"github.com/SisyphusSQ/my2sql/internal/config"
	"github.com/SisyphusSQ/my2sql/internal/log"
	"github.com/SisyphusSQ/my2sql/internal/utils"
	"github.com/SisyphusSQ/my2sql/internal/vars"
)

type DDLPosInfo struct {
	Binlog   string `json:"binlog"`
	StartPos uint32 `json:"start_position"`
	StopPos  uint32 `json:"stop_position"`
	DdlSql   string `json:"ddl_sql"`
}

// KeyInfo {colname1, colname2}
type KeyInfo []string

type FieldInfo struct {
	Index      int    `json:"index"`
	FieldName  string `json:"column_name"`
	FieldType  string `json:"column_type"`
	IsUnsigned bool   `json:"is_unsigned"`
}

type TblInfo struct {
	Database   string       `json:"database"`
	Table      string       `json:"table"`
	Columns    []*FieldInfo `json:"columns"`
	PrimaryKey KeyInfo      `json:"primary_key"`
	UniqueKeys []KeyInfo    `json:"unique_keys"`
	//	DdlInfo    DdlPosInfo  `json:"ddl_info"`
}

type column struct {
	idx      int
	name     string
	notNull  bool
	unsigned bool
}

type table struct {
	schema string
	name   string

	columns      []*column
	indexColumns map[string][]*column
}

type TblColsInfo struct {
	client *sql.DB

	// {db.tb:TblInfo}}
	tableInfos map[string]*TblInfo
}

func NewTblColsInfo(c *config.Config) (*TblColsInfo, error) {
	dsn := fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/?autocommit=true&charset=utf8mb4,utf8,latin1&loc=Local&parseTime=true",
		c.User, c.Passwd, c.Host, c.Port)

	client, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	return &TblColsInfo{
		client:     client,
		tableInfos: make(map[string]*TblInfo),
	}, nil
}

func (t *TblColsInfo) GetTableInfo(schema, table string) *TblInfo {
	absTable := utils.GetAbsTableName(schema, table)

	tbInfo, ok := t.tableInfos[absTable]
	if !ok {
		t.GetTableCols(schema, table)
		tbInfo, ok = t.tableInfos[absTable]
		if !ok {
			log.Logger.Fatal("table struct not found for %s, maybe it was dropped. Skip it", absTable)
		}
	}

	return tbInfo
}

func (t *TblColsInfo) GetTableCols(schema, table string) {
	if utils.IsAnyEmpty(schema, table) {
		log.Logger.Fatal(vars.SchemaTableEmpty.Error())
	}

	absTable := utils.GetAbsTableName(schema, table)

	query := fmt.Sprintf(vars.ShowColumns, schema, table)
	rows, err := t.client.Query(query)
	if err != nil {
		log.Logger.Fatal("table[%s] show columns query failed, err: %v", absTable, err)
	}
	defer rows.Close()

	/*
	   Show an example.

	   mysql> show columns from test.tb;
	   +-------+---------+------+-----+---------+-------+
	   | Field | Type    | Null | Key | Default | Extra |
	   +-------+---------+------+-----+---------+-------+
	   | a     | int(11) | NO   | PRI | NULL    |       |
	   | b     | int(11) | NO   | PRI | NULL    |       |
	   | c     | int(11) | YES  | MUL | NULL    |       |
	   | d     | int(11) | YES  |     | NULL    |       |
	   +-------+---------+------+-----+---------+-------+
	*/

	cols, err := rows.Columns()
	if err != nil {
		log.Logger.Fatal("table[%s] show columns query failed, err: %v", absTable, err)
	}

	i := 0
	fieldInfos := make([]*FieldInfo, 0, len(cols))
	for rows.Next() {
		scanArgs := make([]any, len(cols))
		for i := range scanArgs {
			scanArgs[i] = &sql.RawBytes{}
		}

		if err = rows.Scan(scanArgs...); err != nil {
			log.Logger.Fatal("table[%s] show columns query failed, err: %v", absTable, err)
		}

		typeStr := utils.ColumnValue(scanArgs, cols, "Type")
		f := &FieldInfo{
			Index:      i,
			FieldName:  utils.ColumnValue(scanArgs, cols, "Field"),
			FieldType:  utils.GetFieldType(typeStr),
			IsUnsigned: utils.IsUnsigned(typeStr),
		}

		fieldInfos = append(fieldInfos, f)
		i++
	}

	t.tableInfos[absTable] = &TblInfo{
		Database: schema,
		Table:    table,
		Columns:  fieldInfos,
	}
}

func (t *TblColsInfo) GetTableKeys(schema, table string) {
	var (
		primary = make(KeyInfo, 0)
		unique  = make(map[string]KeyInfo)
	)

	absTable := utils.GetAbsTableName(schema, table)
	query := fmt.Sprintf(vars.ShowKeys, schema, table)
	rows, err := t.client.Query(query)
	if err != nil {
		log.Logger.Fatal("table[%s] show keys query failed, err: %v", absTable, err)
	}
	defer rows.Close()

	// Show an example.
	/*
		mysql> show index from test.t;
		+-------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+
		| Table | Non_unique | Key_name | Seq_in_index | Column_name | Collation | Cardinality | Sub_part | Packed | Null | Index_type | Comment | Index_comment |
		+-------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+
		| t     |          0 | PRIMARY  |            1 | a           | A         |           0 |     NULL | NULL   |      | BTREE      |         |               |
		| t     |          0 | PRIMARY  |            2 | b           | A         |           0 |     NULL | NULL   |      | BTREE      |         |               |
		| t     |          0 | ucd      |            1 | c           | A         |           0 |     NULL | NULL   | YES  | BTREE      |         |               |
		| t     |          0 | ucd      |            2 | d           | A         |           0 |     NULL | NULL   | YES  | BTREE      |         |               |
		+-------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+
	*/

	cols, err := rows.Columns()
	if err != nil {
		log.Logger.Fatal("table[%s] show keys query failed, err: %v", absTable, err)
	}

	for rows.Next() {
		scanArgs := make([]any, len(cols))
		for i := range scanArgs {
			scanArgs[i] = &sql.RawBytes{}
		}

		if err = rows.Scan(scanArgs...); err != nil {
			log.Logger.Fatal("table[%s] show columns query failed, err: %v", absTable, err)
		}

		isUnique, err := utils.ColumnValueInt64(scanArgs, cols, "Non_unique")
		if err != nil {
			log.Logger.Fatal("table[%s] show columns query failed, err: %v", absTable, err)
		} else if isUnique != 0 {
			continue
		}

		keyName := utils.ColumnValue(scanArgs, cols, "Key_name")
		if utils.IsPrimary(keyName) {
			primary = append(primary, utils.ColumnValue(scanArgs, cols, "Key_name"))
		} else {
			if _, ok := unique[keyName]; !ok {
				unique[keyName] = make(KeyInfo, 0)
			}
			unique[keyName] = append(unique[keyName], utils.ColumnValue(scanArgs, cols, "Key_name"))
		}
	}

	tableInfo, ok := t.tableInfos[absTable]
	if !ok {
		log.Logger.Fatal("table[%s] why? not found", absTable)
	}

	tableInfo.PrimaryKey = primary
	for _, u := range unique {
		tableInfo.UniqueKeys = append(tableInfo.UniqueKeys, u)
	}
}

func (t *TblColsInfo) Stop() {
	if t.client != nil {
		_ = t.client.Close()
	}
}
