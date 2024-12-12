package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/SisyphusSQ/my2sql/internal/config"
	"github.com/SisyphusSQ/my2sql/internal/core"
	"github.com/SisyphusSQ/my2sql/internal/locker"
	"github.com/SisyphusSQ/my2sql/internal/log"
	"github.com/SisyphusSQ/my2sql/internal/models"
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

	cpuprofile string
	memprofile string
)

var runCmd = &cobra.Command{
	Use:     "run",
	Short:   "Start to run my2sql-plus tools...",
	Example: fmt.Sprintf("%s run ...\n", "my2sql"),
	RunE: func(cmd *cobra.Command, args []string) error {
		c.ParseConfig(dbs, tbs, ignoreDBs, ignoreTBs, sqlTypes, startTime, stopTime, doNotAddPrefixDB)

		var (
			totalRoutines   int
			finishRoutines  int
			isClosed        bool
			trCnt           atomic.Int64
			extractWg       sync.WaitGroup
			transWg         sync.WaitGroup
			loadWg          sync.WaitGroup
			tbColsInfo, err = models.NewTblColsInfo(c)
			errChan         = make(chan error)
			lock            = locker.NewTrxLock()
			ctx, cancel     = context.WithCancel(context.Background())
			chanCap         = c.Threads * 2
			eventChan       = make(chan *models.MyBinEvent, chanCap)
			statChan        = make(chan *models.BinEventStats, chanCap)
			sqlChan         = make(chan *models.ResultSQL)
			jsonChan        = make(chan *models.ResultSQL)
		)

		if err != nil {
			cancel()
			return err
		}
		defer tbColsInfo.Stop()

		go func() {
			totalRoutines++
			statsLoader, err := core.NewLoader("stats", &loadWg, ctx, c, sqlChan, jsonChan, statChan)
			if err != nil {
				log.Logger.Fatal("create %s loader failed, err: %v", "stats", err)
			}

			errChan <- statsLoader.Start()
			statsLoader.Stop()
		}()

		if c.WorkType != "stats" {
			for _, t := range []string{c.WorkType, "json"} {
				totalRoutines++
				load, err := core.NewLoader(t, &loadWg, ctx, c, sqlChan, jsonChan, statChan)
				if err != nil {
					log.Logger.Error("create %s loader failed, err: %v", t, err)
					cancel()
					return err
				}

				go func() {
					errChan <- load.Start()
					load.Stop()
				}()
			}

			for i := range c.Threads {
				totalRoutines++
				transform := core.NewTransformer("default", &transWg, ctx, i, &trCnt, c, tbColsInfo, eventChan,
					sqlChan, jsonChan, lock)

				go func() {
					errChan <- transform.Start()
					transform.Stop()
					//trCnt.Add(1)
				}()
			}
		}

		totalRoutines++
		extract := core.NewExtractor(c.Mode, &extractWg, ctx, c, eventChan, statChan)
		go func() {
			errChan <- extract.Start()
			extract.Stop()
		}()

		f := StartCpuProfile()
		defer StopCpuProfile(f)

		// finish cpu perf profiling before ctrl-C/kill/kill -15
		ch := make(chan os.Signal, 5)
		signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
		go func() {
			errChan <- func() error {
				for {
					sig := <-ch
					switch sig {
					case syscall.SIGINT, syscall.SIGTERM:
						cancel()
						log.Logger.Debug("Terminating process, will finish cpu pprof before exit(if specified)...")
						StopCpuProfile(f)
						return vars.ManualKill
					default:
						// do nothing
					}
				}
			}()
		}()

		for {
			select {
			case err := <-errChan:
				if err != nil {
					cancel()
					log.Logger.Error("my2sql-plus got err, err: %v", err)

					if errors.Is(err, vars.ManualKill) {
						continue
					}
				}

				finishRoutines++
				if trCnt.Load() == int64(c.Threads) {
					if !isClosed {
						close(sqlChan)
						close(jsonChan)
						isClosed = true
					}
				}

				if totalRoutines == finishRoutines {
					goto exit
				}
			}
		}

	exit:
		cancel()
		extractWg.Wait()
		transWg.Wait()
		loadWg.Wait()

		// do memory profiling before exit
		MemProfile()
		return err
	},
}

func StartCpuProfile() *os.File {
	if cpuprofile != "" {
		f, err := os.Create(cpuprofile)
		if err != nil {
			log.Logger.Fatal("could not create CPU profile: ", err)
		}
		if err = pprof.StartCPUProfile(f); err != nil {
			log.Logger.Fatal("could not start CPU profile: ", err)
		}
		log.Logger.Info("cpu pprof start ...")
		return f
	}
	return nil
}

func StopCpuProfile(f *os.File) {
	if f != nil {
		pprof.StopCPUProfile()
		f.Close()
		log.Logger.Info("cpu pprof stopped [file=%s]!", cpuprofile)
		return
	}
}

func MemProfile() {
	if memprofile != "" {
		f, err := os.Create(memprofile)
		if err != nil {
			log.Logger.Fatal("could not create memory profile: ", err)
		}
		defer f.Close()
		runtime.GC() // get up-to-date statistics
		if err = pprof.WriteHeapProfile(f); err != nil {
			log.Logger.Fatal("could not write memory profile: ", err)
		}
		log.Logger.Info("mem pprof done [file=%s]!", memprofile)
	}
}

func initRun() {
	runCmd.Flags().StringVar(&cpuprofile, "cpuprofile", "", "write cpu profile to `file`")
	runCmd.Flags().StringVar(&memprofile, "memprofile", "", "write memory profile to `file`")

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

	runCmd.Flags().IntVar(&c.Threads, "threads", vars.GetDefaultValueOfRange("Threads"), "Works with -workType=2sql|rollback. threads to run")
	rootCmd.AddCommand(runCmd)
}
