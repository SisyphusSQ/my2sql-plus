package loader

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/SisyphusSQ/my2sql/internal/config"
	"github.com/SisyphusSQ/my2sql/internal/log"
	"github.com/SisyphusSQ/my2sql/internal/models"
	"github.com/SisyphusSQ/my2sql/internal/utils"
)

const (
	rbFilePrefix = "rollback"
	fdFilePrefix = "forward"
	jsFilePrefix = "json"
)

type SQLLoader struct {
	wg  *sync.WaitGroup
	ctx context.Context

	typeName   string
	lastBinlog string

	baseDir string
	file    *os.File
	writer  *bufio.Writer

	isRollBack    bool
	isPrintScreen bool
	isPrintExtra  bool

	isFilePerTable bool
	fileMap        map[string]*os.File
	writerMap      map[string]*bufio.Writer

	sqlChan <-chan *models.ResultSQL
}

func NewSQLLoader(wg *sync.WaitGroup, ctx context.Context, c *config.Config,
	typeName string, sqlChan chan *models.ResultSQL) *SQLLoader {
	s := &SQLLoader{
		wg:       wg,
		ctx:      ctx,
		typeName: typeName,

		baseDir:        c.OutputDir,
		isRollBack:     c.WorkType == "rollback",
		isPrintScreen:  c.OutputToScreen,
		isPrintExtra:   c.PrintExtraInfo,
		isFilePerTable: c.FilePerTable,

		sqlChan: sqlChan,
	}

	if s.isFilePerTable {
		s.fileMap = make(map[string]*os.File)
		s.writerMap = make(map[string]*bufio.Writer)
	}

	return s
}

func (s *SQLLoader) Start() error {
	var err error
	s.wg.Add(1)
	log.Logger.Info("start thread to write redo/rollback sql into file")
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return nil
		case <-ticker.C:
			if s.isFilePerTable {
				for _, w := range s.writerMap {
					_ = w.Flush()
				}
			} else {
				if s.writer != nil {
					_ = s.writer.Flush()
				}
			}
		case sql, ok := <-s.sqlChan:
			if !ok {
				return nil
			}

			if s.isPrintScreen {
				fmt.Println(strings.Join(sql.SQLs, ";\n") + ";")
				continue
			}

			if s.isFilePerTable {
				err = s.handleSQLPerTable(sql)
			} else {
				err = s.handleSQL(sql)
			}

			if err != nil {
				log.Logger.Error("handle sql error, err: %v", err)
				return err
			}
		}
	}
}

func (s *SQLLoader) handleSQL(sql *models.ResultSQL) error {
	var err error

	if s.lastBinlog == "" {
		s.lastBinlog = sql.SQLInfo.Binlog
	} else if s.lastBinlog != sql.SQLInfo.Binlog {
		log.Logger.Info("finish processing %s %d", s.lastBinlog, sql.SQLInfo.EndPos)
		s.lastBinlog = sql.SQLInfo.Binlog

		// flush and new one
		_ = s.writer.Flush()
		_ = s.file.Close()

		s.file = nil
	}

	if s.file == nil {
		fileName := s.getAbsFilename(sql.SQLInfo)
		if s.file, err = os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644); err != nil {
			log.Logger.Error("open file error, err: %v", err)
			return err
		}

		s.writer = bufio.NewWriter(s.file)
	}

	return s.writeSQL2File(s.writer, sql)
}

func (s *SQLLoader) handleSQLPerTable(sql *models.ResultSQL) error {
	var err error

	if s.lastBinlog == "" {
		s.lastBinlog = sql.SQLInfo.Binlog
	} else if s.lastBinlog != sql.SQLInfo.Binlog {
		log.Logger.Info("finish processing %s %d", s.lastBinlog, sql.SQLInfo.EndPos)
		s.lastBinlog = sql.SQLInfo.Binlog
	}

	fileName := s.getAbsFilename(sql.SQLInfo)
	if _, ok := s.fileMap[fileName]; !ok {
		if s.fileMap[fileName], err = os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644); err != nil {
			log.Logger.Error("open file error, err: %v", err)
			return err
		}

		s.writerMap[fileName] = bufio.NewWriter(s.file)
	}

	return s.writeSQL2File(s.writerMap[fileName], sql)
}

func (s *SQLLoader) writeSQL2File(writer *bufio.Writer, sql *models.ResultSQL) error {
	// sqls must be sequentiality
	var (
		err   error
		lines string
		extra = sql.SQLInfo
	)

	if s.typeName == jsFilePrefix {
		lines = strings.Join(sql.Jsons, "\n") + "\n"
		goto write
	}

	if s.isPrintExtra {
		lines = fmt.Sprintf("-- datetime=%s database=%s table=%s binlog=%s startpos=%d stoppos=%d\n",
			extra.Datetime, extra.Schema, extra.Table, extra.Binlog, extra.StartPos, extra.EndPos)
	}
	lines += strings.Join(sql.SQLs, ";\n") + ";\n"

write:
	_, err = writer.WriteString(lines)
	if err != nil {
		log.Logger.Error("write file error, err: %v", err)
		return err
	}
	return nil
}

func (s *SQLLoader) getAbsFilename(i models.ExtraInfo) string {
	_, idx := utils.GetLogNameAndIndex(s.lastBinlog)

	if s.isFilePerTable {
		return filepath.Join(s.baseDir, fmt.Sprintf("%s.%s.%s.%d.sql", i.Schema, i.Table, s.typeName, idx))
	} else {
		return filepath.Join(s.baseDir, fmt.Sprintf("%s.%d.sql", s.typeName, idx))
	}
}

func (s *SQLLoader) LastBinlog() string {
	return s.lastBinlog
}

func (s *SQLLoader) Stop() {
	log.Logger.Info("finish writing redo/%s sql into file", s.typeName)
	if s.file != nil {
		_ = s.writer.Flush()
		_ = s.file.Close()
	}

	if s.fileMap != nil {
		for file, f := range s.fileMap {
			_ = s.writerMap[file].Flush()
			_ = f.Close()
		}
	}

	s.wg.Done()
	log.Logger.Info("exit thread to write redo/%s sql into file", s.typeName)
}
