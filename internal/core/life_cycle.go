package core

type Parser interface {
	Start() error
	Binlog() string
	Stop()
}
