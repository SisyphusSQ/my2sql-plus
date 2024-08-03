package vars

import (
	"fmt"
	"time"

	"github.com/SisyphusSQ/my2sql/internal/log"
)

const (
	ValidOptMsg  = "valid options are: "
	JoinSepComma = ","

	EventTimeout = 5 * time.Second
)

const (
	TrxProcess = iota - 1
	TrxBegin
	TrxCommit
	TrxRollback
)

const (
	ReProcess = iota
	ReContinue
	ReBreak
	ReFileEnd
)

var (
	GOptsValidMode      = []string{"repl", "file"}
	GOptsValidWorkType  = []string{"2sql", "rollback", "stats"}
	GOptsValidDBType    = []string{"mysql", "mariadb"}
	GOptsValidFilterSQL = []string{"insert", "update", "delete"}

	GOptsValueRange = map[string][]int{
		"PrintInterval":  {1, 600, 30},
		"BigTrxRowLimit": {1, 30000, 10},
		"LongTrxSeconds": {0, 3600, 1},
		"InsertRows":     {1, 500, 30},
		"Threads":        {1, 16, 2},
	}
)

func GetMinValueOfRange(opt string) int {
	return GOptsValueRange[opt][0]
}

func GetMaxValueOfRange(opt string) int {
	return GOptsValueRange[opt][1]
}

func GetDefaultValueOfRange(opt string) int {
	return GOptsValueRange[opt][2]
}

func GetDefaultAndRangeValueMsg(opt string) string {
	return fmt.Sprintf("Valid values range from %d to %d, default %d",
		GetMinValueOfRange(opt),
		GetMaxValueOfRange(opt),
		GetDefaultValueOfRange(opt),
	)
}

func CheckValueInRange(opt string, val int, prefix string, ifExt bool) bool {
	valOk := true
	if val < GetMinValueOfRange(opt) {
		valOk = false
	} else if val > GetMaxValueOfRange(opt) {
		valOk = false
	}

	if !valOk {
		if ifExt {
			log.Logger.Fatal("%s: %d is specified, but %s", prefix, val, GetDefaultAndRangeValueMsg(opt))
		} else {
			log.Logger.Fatal("%s: %d is specified, but %s", prefix, val, GetDefaultAndRangeValueMsg(opt))
		}
	}
	return valOk
}
