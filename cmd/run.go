package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/SisyphusSQ/my2sql/internal/config"
	"github.com/SisyphusSQ/my2sql/internal/utils"
	"github.com/SisyphusSQ/my2sql/internal/vars"
)

var (
	c = config.New()

	dbs       string
	tbs       string
	ignoreDBs string
	ignoreTBs string

	sqlTypes         string
	startTime        string
	stopTime         string
	doNotAddPrefixDB bool
)

var runCmd = &cobra.Command{
	Use:     "run",
	Short:   "Start to run my2sql-plus tools...",
	Example: fmt.Sprintf("%s run ...\n", "my2sql"),
	RunE: func(cmd *cobra.Command, args []string) error {
		c.ParseConfig(dbs, tbs, ignoreDBs, ignoreTBs, sqlTypes, startTime, stopTime, doNotAddPrefixDB)
		return nil
	},
}

func initRun() {
	runCmd.Flags().StringVar(&c.Mode, "mode", "repl", utils.SliceToString(vars.GOptsValidMode, vars.JoinSepComma, vars.ValidOptMsg)+". repl: as a slave to get binlogs from master. file: get binlogs from local filesystem. default repl")
	runCmd.Flags().StringVar(&c.WorkType, "work-type", "2sql", utils.SliceToString(vars.GOptsValidWorkType, vars.JoinSepComma, vars.ValidOptMsg)+". 2sql: convert binlog to sqls, rollback: generate rollback sqls, stats: analyze transactions. default: 2sql")
	runCmd.Flags().StringVar(&c.MySQLType, "mysql-type", "mysql", utils.SliceToString(vars.GOptsValidDBType, vars.JoinSepComma, vars.ValidOptMsg)+". server of binlog, mysql or mariadb, default mysql")

	runCmd.Flags().StringVar(&c.Host, "host", "127.0.0.1", "mysql host, default 127.0.0.1 .")
	runCmd.Flags().UintVar(&c.Port, "port", 3306, "mysql port, default 3306.")
	runCmd.Flags().StringVar(&c.User, "user", "", "mysql user. ")
	runCmd.Flags().StringVar(&c.Passwd, "password", "", "mysql user password.")
	runCmd.Flags().UintVar(&c.ServerId, "server-id", 1113306, "this program replicates from mysql as slave to read binlogs. Must set this server id unique from other slaves, default 1113306")

	runCmd.Flags().StringVar(&dbs, "databases", "", "only parse these databases, comma seperated, default all.")
	runCmd.Flags().StringVar(&tbs, "tables", "", "only parse these tables, comma seperated, DONOT prefix with schema, default all.")
	runCmd.Flags().StringVar(&ignoreDBs, "ignore-databases", "", "ignore parse these databases, comma seperated, default null")
	runCmd.Flags().StringVar(&ignoreTBs, "ignore-tables", "", "ignore parse these tables, comma seperated, default null")
	runCmd.Flags().StringVar(&sqlTypes, "sql", "", utils.SliceToString(vars.GOptsValidFilterSQL, vars.JoinSepComma, vars.ValidOptMsg)+". only parse these types of sql, comma seperated, valid types are: insert, update, delete; default is all(insert,update,delete)")
	runCmd.Flags().BoolVar(&c.IgnorePrimaryKeyForInsert, "ignore-primaryKey-forInsert", false, "for insert statement when -workType=2sql, ignore primary key")

	runCmd.Flags().StringVar(&c.StartFile, "start-file", "", "binlog file to start reading")
	runCmd.Flags().UintVar(&c.StartPos, "start-pos", 4, "start reading the binlog at position")
	runCmd.Flags().StringVar(&c.StopFile, "stop-file", "", "binlog file to stop reading")
	runCmd.Flags().UintVar(&c.StopPos, "stop-pos", 4, "Stop reading the binlog at position")
	runCmd.Flags().StringVar(&c.LocalBinFile, "local-binlog-file", "", "local binlog files to process, It works with -mode=file ")

	runCmd.Flags().StringVar(&c.BinlogTimeLocation, "tl", "Local", "time location to parse timestamp/datetime column in binlog, such as Asia/Shanghai. default Local")
	runCmd.Flags().StringVar(&startTime, "start-datetime", "", "Start reading the binlog at first event having a datetime equal or posterior to the argument, it should be like this: \"2020-01-01 01:00:00\"")
	runCmd.Flags().StringVar(&stopTime, "stop-datetime", "", "Stop reading the binlog at first event having a datetime equal or posterior to the argument, it should be like this: \"2020-12-30 01:00:00\"")

	runCmd.Flags().BoolVar(&c.OutputToScreen, "output-toScreen", false, "Just output to screen,do not write to file")
	runCmd.Flags().BoolVar(&c.PrintExtraInfo, "add-extraInfo", false, "Works with -work-type=2sql|rollback. Print database/table/datetime/binlog_position...info on the line before sql, default false")

	runCmd.Flags().BoolVar(&c.FullColumns, "full-columns", false, "For update sql, include unchanged columns. for update and delete, use all columns to build where condition.\t\ndefault false, this is, use changed columns to build set part, use primary/unique key to build where condition")
	runCmd.Flags().BoolVar(&doNotAddPrefixDB, "do-not-add-prefixDb", false, "Prefix table name witch database name in sql,ex: insert into db1.tb1 (x1, x1) values (y1, y1). ")
	runCmd.Flags().BoolVar(&c.UseUniqueKeyFirst, "U", false, "prefer to use unique key instead of primary key to build where condition for delete/update sql")

	runCmd.Flags().StringVar(&c.OutputDir, "output-dir", "", "result output dir, default current work dir. Attention, result files could be large, set it to a dir with large free space")
	runCmd.Flags().BoolVar(&c.FilePerTable, "file-per-table", false, "One file for one table if true, else one file for all tables. default false. Attention, always one file for one binlog")
	runCmd.Flags().IntVar(&c.PrintInterval, "print-interval", vars.GetDefaultValueOfRange("PrintInterval"), "works with -w='stats', print stats info each PrintInterval. "+vars.GetDefaultAndRangeValueMsg("PrintInterval"))
	runCmd.Flags().IntVar(&c.BigTrxRowLimit, "big-trx-row-limit", vars.GetDefaultValueOfRange("BigTrxRowLimit"), "transaction with affected rows greater or equal to this value is considerated as big transaction. "+vars.GetDefaultAndRangeValueMsg("BigTrxRowLimit"))
	runCmd.Flags().IntVar(&c.LongTrxSeconds, "long-trx-seconds", vars.GetDefaultValueOfRange("LongTrxSeconds"), "transaction with duration greater or equal to this value is considerated as long transaction. "+vars.GetDefaultAndRangeValueMsg("LongTrxSeconds"))

	runCmd.Flags().UintVar(&c.Threads, "threads", uint(vars.GetDefaultValueOfRange("Threads")), "Works with -workType=2sql|rollback. threads to run")
	rootCmd.AddCommand(runCmd)
}
