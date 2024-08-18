package models

type DBTable struct {
	Database string
	Table    string
}

func (d DBTable) Copy() DBTable {
	return DBTable{
		Database: d.Database,
		Table:    d.Table,
	}
}

type SQLInfo struct {
	Tables      []DBTable
	UseDatabase string
	SqlStr      string
	SqlType     int
}

type ExtraInfo struct {
	Schema    string
	Table     string
	Binlog    string
	StartPos  uint32
	EndPos    uint32
	Datetime  string
	TrxIndex  uint64
	TrxStatus int
}

type ResultSQL struct {
	SQLs    []string
	SQLInfo ExtraInfo
}

type JsonEvent struct {
	EventType  string         `json:"eventType"`
	SchemaName string         `json:"schemaName"`
	TableName  string         `json:"tableName"`
	Timestamp  uint32         `json:"timestamp"`
	Position   string         `json:"position"`
	RowBefore  map[string]any `json:"rowBefore,omitempty"`
	RowAfter   map[string]any `json:"rowAfter,omitempty"`
}
