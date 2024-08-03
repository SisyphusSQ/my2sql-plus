package utils

import (
	"fmt"
	"github.com/SisyphusSQ/my2sql/internal/log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func CheckIsDir(fd string) (bool, string) {
	fs, err := os.Stat(fd)
	if err != nil {
		return false, fd + " not exists"
	}
	if fs.IsDir() {
		return true, ""
	} else {
		return false, fd + " is not a dir"
	}
}

// IsFile checks whether the path is a file,
// it returns false when it's a directory or does not exist.
func IsFile(fp string) bool {
	f, e := os.Stat(fp)
	if e != nil {
		return false
	}
	return !f.IsDir()
}

func GetLogNameAndIndex(binlog string) (string, int) {
	binlogFile := filepath.Base(binlog)
	arr := strings.Split(binlogFile, ".")
	cnt := len(arr)
	n, err := strconv.ParseUint(arr[cnt-1], 10, 32)
	if err != nil {
		log.Logger.Fatal("parse binlog file index number error %v", err)
	}
	indx := int(n)
	baseName := strings.Join(arr[0:cnt-1], "")
	return baseName, indx
}

func GetNextBinlog(baseName string, indx *int) string {
	*indx++
	idxStr := fmt.Sprintf("%06d", *indx)
	return baseName + "." + idxStr
}
