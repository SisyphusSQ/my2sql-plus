package config

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/SisyphusSQ/my2sql/internal/log"
	"github.com/SisyphusSQ/my2sql/internal/utils"
	"github.com/SisyphusSQ/my2sql/internal/utils/timeutil"
	"github.com/SisyphusSQ/my2sql/internal/vars"
)

const (
	filterDTTemp = "%s.%s"
	filterDBTemp = "%s.all"
	filterTBTemp = "all.%s"
)

type Config struct {
	Mode      string
	WorkType  string
	MySQLType string

	Host     string
	Port     uint
	User     string
	Passwd   string
	ServerId uint

	IsAssign  bool
	AssignDB  bool
	AssignTB  bool
	AssignMap map[string]struct{}

	IsIgnore  bool
	IgnoreDB  bool
	IgnoreTB  bool
	IgnoreMap map[string]struct{}

	FilterSQL    []string
	FilterSQLLen int
	FilterSQLMap map[string]struct{}

	StartFile         string
	StartPos          uint
	StartFilePos      mysql.Position
	IfSetStartFilePos bool

	StopFile         string
	StopPos          uint
	StopFilePos      mysql.Position
	IfSetStopFilePos bool

	StartDatetime      uint32
	StopDatetime       uint32
	BinlogTimeLocation string
	GTimeLocation      *time.Location

	IfSetStartDateTime bool
	IfSetStopDateTime  bool

	LocalBinFile string

	OutputToScreen bool
	PrintInterval  int
	BigTrxRowLimit int
	LongTrxSeconds int

	IfSetStopParsPoint bool

	OutputDir string

	FullColumns    bool
	InsertRows     int
	KeepTrx        bool
	SQLTblPrefixDB bool
	FilePerTable   bool

	PrintExtraInfo bool

	Threads int

	ReadTblDefJsonFile string
	OnlyColFromFile    bool
	DumpTblDefToFile   string

	BinlogDir string

	GivenBinlogFile string

	UseUniqueKeyFirst         bool
	IgnorePrimaryKeyForInsert bool
	ReplaceIntoForInsert      bool

	ParseStatementSql bool

	IgnoreParsedErrForSql string // if parsed error, for sql match this regexp, only print error info, but not exits
	IgnoreParsedErrRegexp *regexp.Regexp
}

func New() *Config {
	return &Config{}
}

func (c *Config) ParseConfig(dbs, tbs, ignoreDBs, ignoreTBs, sqlTypes, startTime, stopTime string, doNotAddPrefixDB bool) {
	var err error

	if c.Mode != "repl" && c.Mode != "file" {
		log.Logger.Fatal("unsupported mode=%s, valid modes: file, repl", c.Mode)
	}

	// check --output-dir
	if c.OutputDir != "" {
		ifExist, errMsg := utils.CheckIsDir(c.OutputDir)
		if !ifExist {
			log.Logger.Fatal("OutputDir -o=%s DIR_NOT_EXISTS", errMsg)
		}
	} else {
		c.OutputDir, _ = os.Getwd()
	}

	if !doNotAddPrefixDB {
		c.SQLTblPrefixDB = true
	} else {
		c.SQLTblPrefixDB = false
	}

	c.AssignMap = make(map[string]struct{})
	dbArr := utils.CommaListToArray(dbs)
	tbArr := utils.CommaListToArray(tbs)
	if len(dbArr) > 0 {
		c.IsAssign = true
		c.AssignDB = true
		for _, db := range dbArr {
			if len(tbArr) > 0 {
				c.AssignTB = true
				for _, tb := range tbArr {
					c.AssignMap[fmt.Sprintf(filterDTTemp, db, tb)] = struct{}{}
				}
			} else {
				c.AssignMap[fmt.Sprintf(filterDBTemp, db)] = struct{}{}
			}
		}
	} else {
		if len(tbArr) > 0 {
			c.IsAssign = true
			c.AssignDB = true
			for _, tb := range tbArr {
				c.AssignMap[fmt.Sprintf(filterTBTemp, tb)] = struct{}{}
			}
		}
	}

	c.IgnoreMap = make(map[string]struct{})
	ignoreDBArr := utils.CommaListToArray(ignoreDBs)
	ignoreTBArr := utils.CommaListToArray(ignoreTBs)
	if len(ignoreDBArr) > 0 {
		c.IsIgnore = true
		c.IgnoreDB = true
		for _, db := range ignoreDBArr {
			if len(ignoreTBArr) > 0 {
				c.IgnoreTB = true
				for _, tb := range ignoreTBArr {
					c.IgnoreMap[fmt.Sprintf(filterDTTemp, db, tb)] = struct{}{}
				}
			} else {
				c.IgnoreMap[fmt.Sprintf(filterDBTemp, db)] = struct{}{}
			}
		}
	} else {
		if len(ignoreTBArr) > 0 {
			c.IsIgnore = true
			c.IgnoreTB = true
			for _, tb := range ignoreTBArr {
				c.IgnoreMap[fmt.Sprintf(filterTBTemp, tb)] = struct{}{}
			}
		}
	}

	c.FilterSQLMap = make(map[string]struct{})
	if sqlTypes != "" {
		c.FilterSQL = utils.CommaListToArray(sqlTypes)
		for _, t := range c.FilterSQL {
			utils.CheckItemInSlice(vars.GOptsValidFilterSQL, t, "invalid sqltypes", true)
			c.FilterSQLMap[t] = struct{}{}
		}
		c.FilterSQLLen = len(c.FilterSQL)
	} else {
		c.FilterSQLLen = 0
		for _, t := range vars.GOptsValidFilterSQL {
			c.FilterSQLMap[t] = struct{}{}
		}
	}

	c.GTimeLocation, err = time.LoadLocation(c.BinlogTimeLocation)
	if err != nil {
		log.Logger.Fatal("invalid time location[%s], err: %v"+c.BinlogTimeLocation, err)
	}

	if startTime != "" {
		t, err := time.ParseInLocation(timeutil.TimeLayout, startTime, c.GTimeLocation)
		if err != nil {
			log.Logger.Fatal("invalid start datetime -start-datetime: %s " + startTime)
		}
		c.StartDatetime = uint32(t.Unix())
		c.IfSetStartDateTime = true
	} else {
		c.IfSetStartDateTime = false
	}

	if stopTime != "" {
		t, err := time.ParseInLocation(timeutil.TimeLayout, stopTime, c.GTimeLocation)
		if err != nil {
			log.Logger.Fatal("invalid stop datetime -stop-datetime: %s " + stopTime)
		}
		c.StopDatetime = uint32(t.Unix())
		c.IfSetStopDateTime = true
	} else {
		c.IfSetStopDateTime = false
	}

	if startTime != "" && stopTime != "" {
		if c.StartDatetime >= c.StopDatetime {
			log.Logger.Fatal("-start-datetime must be earlier than -stop-datetime")
		}
	}

	if c.StartFile != "" {
		c.IfSetStartFilePos = true
		c.StartFilePos = mysql.Position{Name: c.StartFile, Pos: uint32(c.StartPos)}
	} else {
		c.IfSetStartFilePos = false
	}

	if c.StopFile != "" {
		c.IfSetStopFilePos = true
		c.StopFilePos = mysql.Position{Name: c.StopFile, Pos: uint32(c.StopPos)}
		c.IfSetStopParsPoint = true
	} else {
		c.IfSetStopFilePos = false
		c.IfSetStopParsPoint = false
	}

	if c.Mode == "file" {
		if c.StartFile == "" {
			log.Logger.Fatal("missing binlog file.  -start-file must be specify when -mode=file ")
		}
		c.GivenBinlogFile = c.StartFile
		if !utils.IsFile(c.GivenBinlogFile) {
			log.Logger.Fatal("%s doesn't exists nor a file", c.GivenBinlogFile)
		} else {
			c.BinlogDir = filepath.Dir(c.GivenBinlogFile)
		}
	}

	if c.Mode == "file" {
		if c.LocalBinFile == "" {
			log.Logger.Fatal("missing binlog file.  -local-binlog-file must be specify when -mode=file ")
		}
		c.GivenBinlogFile = c.LocalBinFile
		if !utils.IsFile(c.GivenBinlogFile) {
			log.Logger.Fatal("%s doesn't exists nor a file\n", c.GivenBinlogFile)
		} else {
			c.BinlogDir = filepath.Dir(c.GivenBinlogFile)
		}
	}

	//check -mode
	utils.CheckItemInSlice(vars.GOptsValidMode, c.Mode, "invalid arg for -mode", true)

	//check -workType
	utils.CheckItemInSlice(vars.GOptsValidWorkType, c.WorkType, "invalid arg for -workType", true)

	//check -mysqlType
	utils.CheckItemInSlice(vars.GOptsValidDBType, c.MySQLType, "invalid arg for -mysqlType", true)

	if c.StartFile != "" {
		c.StartFile = filepath.Base(c.StartFile)
	}
	if c.StopFile != "" {
		c.StopFile = filepath.Base(c.StopFile)
	}

	//check --start-binlog --start-pos --stop-binlog --stop-pos
	if c.StartFile != "" && c.StartPos != 0 && c.StopFile != "" && c.StopPos != 0 {
		cmpRes := compareBinlogPos(c.StartFile, c.StartPos, c.StopFile, c.StopPos)
		if cmpRes != -1 {
			log.Logger.Fatal("start position(-start-file -start-pos) must less than stop position(-end-file -end-pos)")
		}
	}

	// check --interval
	if c.PrintInterval != vars.GetDefaultValueOfRange("PrintInterval") {
		vars.CheckValueInRange("PrintInterval", c.PrintInterval, "value of -i out of range", true)
	}

	// check --big-trx-rows
	if c.BigTrxRowLimit != vars.GetDefaultValueOfRange("BigTrxRowLimit") {
		vars.CheckValueInRange("BigTrxRowLimit", c.BigTrxRowLimit, "value of -b out of range", true)
	}

	// check --long-trx-seconds
	if c.LongTrxSeconds != vars.GetDefaultValueOfRange("LongTrxSeconds") {
		vars.CheckValueInRange("LongTrxSeconds", c.LongTrxSeconds, "value of -l out of range", true)
	}

	// check --threads
	if c.Threads != vars.GetDefaultValueOfRange("Threads") {
		vars.CheckValueInRange("Threads", c.Threads, "value of -t out of range", true)
	}
}

func (c *Config) DBTBExist(db, tb, fType string) bool {
	if fType == "assign" {
		if c.AssignDB && c.AssignTB {
			_, ok := c.AssignMap[fmt.Sprintf(filterDTTemp, db, tb)]
			return ok
		} else if c.AssignDB {
			_, ok := c.AssignMap[fmt.Sprintf(filterDBTemp, db)]
			return ok
		} else if c.AssignTB {
			_, ok := c.AssignMap[fmt.Sprintf(filterTBTemp, tb)]
			return ok
		}

		return false
	} else {
		if !(c.IgnoreDB && c.IgnoreTB) {
			return false
		}

		if c.IgnoreDB && c.IgnoreTB {
			_, ok := c.IgnoreMap[fmt.Sprintf(filterDTTemp, db, tb)]
			return ok
		} else if c.IgnoreDB {
			_, ok := c.IgnoreMap[fmt.Sprintf(filterDBTemp, db)]
			return ok
		} else if c.IgnoreTB {
			_, ok := c.IgnoreMap[fmt.Sprintf(filterTBTemp, tb)]
			return ok
		}

		return false
	}
}

func compareBinlogPos(sBinFile string, sPos uint, eBinFile string, ePos uint) int {
	// 1: greater, -1: less, 0: equal
	sp := mysql.Position{Name: sBinFile, Pos: uint32(sPos)}
	ep := mysql.Position{Name: eBinFile, Pos: uint32(ePos)}

	return sp.Compare(ep)
}
