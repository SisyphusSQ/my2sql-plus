package core

import (
	"context"
	"sync"

	"github.com/SisyphusSQ/my2sql/internal/config"
	"github.com/SisyphusSQ/my2sql/internal/extractor"
	"github.com/SisyphusSQ/my2sql/internal/loader"
	"github.com/SisyphusSQ/my2sql/internal/locker"
	"github.com/SisyphusSQ/my2sql/internal/models"
	"github.com/SisyphusSQ/my2sql/internal/transformer"
)

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

func NewExtractor(extractType string, wg *sync.WaitGroup, ctx context.Context, c *config.Config,
	eventChan chan *models.MyBinEvent,
	statChan chan *models.BinEventStats) Extractor {
	switch extractType {
	case "file":
		return extractor.NewFileExtract(wg, ctx, c, eventChan, statChan)
	case "repl":
		return extractor.NewReplExtract(wg, ctx, c, eventChan, statChan)
	default:
		panic("unknown extract type: " + extractType)
	}
}

func NewTransformer(transType string, wg *sync.WaitGroup, ctx context.Context, threadNum int,
	c *config.Config, tbColsInfo *models.TblColsInfo, eventChan chan *models.MyBinEvent, sqlChan chan *models.ResultSQL,
	trxLock *locker.TrxLock) Transformer {
	switch transType {
	case "default":
		return transformer.NewTransformer(wg, ctx, threadNum, c, tbColsInfo, eventChan, sqlChan, trxLock)
	default:
		panic("unknown transformer type: " + transType)
	}
}

func NewLoader(loaderType string, wg *sync.WaitGroup, ctx context.Context, c *config.Config,
	typeName string, sqlChan chan *models.ResultSQL, statsChan chan *models.BinEventStats) (Loader, error) {
	switch loaderType {
	case "stats":
		l, err := loader.NewStatsLoader(wg, ctx, c, statsChan)
		if err != nil {
			return nil, err
		}
		return l, nil
	case "binlog":
		return loader.NewSQLLoader(wg, ctx, c, typeName, sqlChan), nil
	default:
		panic("unknown loader type: " + loaderType)
	}
}
