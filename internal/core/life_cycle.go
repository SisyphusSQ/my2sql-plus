package core

type LifeCycle interface {
	Start() error

	Stop()
}

type Extractor interface {
	LifeCycle

	Binlog() string
}

type Transformer interface {
	LifeCycle

	CurPos() string
}

type Loader interface {
	LifeCycle

	LastBinlog() string
}
