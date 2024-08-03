package utils

import (
	"database/sql"

	_ "github.com/go-sql-driver/mysql"
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
