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

type ExtraSQLInfo struct {
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
	SQLInfo ExtraSQLInfo
}
