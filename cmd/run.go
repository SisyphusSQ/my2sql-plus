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

type routineResult struct {
	name string
	err  error
}

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
	Short:   runHelpShort,
	Long:    runHelpDescription,
	Example: runHelpExamples,
	RunE: func(cmd *cobra.Command, args []string) error {
		c.ParseConfig(dbs, tbs, ignoreDBs, ignoreTBs, sqlTypes, startTime, stopTime, doNotAddPrefixDB)

		var (
			totalRoutines   int
			finishRoutines  int
			transformDone   int
			eventChanClosed bool
			statChanClosed  bool
			sqlChanClosed   bool
			flashbackClosed bool
			runErr          error
			trCnt           atomic.Int64
			extractWg       sync.WaitGroup
			transWg         sync.WaitGroup
			loadWg          sync.WaitGroup
			tbColsInfo, err = models.NewTblColsInfo(c)
			errChan         = make(chan routineResult, c.Threads+6)
			lock            = locker.NewTrxLock()
			ctx, cancel     = context.WithCancel(context.Background())
			chanCap         = c.Threads * 2
			eventChan       = make(chan *models.MyBinEvent, chanCap)
			statChan        = make(chan *models.BinEventStats, chanCap)
			sqlChan         = make(chan *models.ResultSQL)
			jsonChan        = make(chan *models.ResultSQL)
			flashbackChan   chan *models.FlashbackEvent
		)

		if c.FlashbackBinlog {
			flashbackChan = make(chan *models.FlashbackEvent, chanCap)
		}

		if err != nil {
			cancel()
			return err
		}
		defer tbColsInfo.Stop()

		totalRoutines++
		go func() {
			statsLoader, err := core.NewLoader("stats", &loadWg, ctx, c, sqlChan, jsonChan, statChan, flashbackChan)
			if err != nil {
				log.Logger.Fatal("create %s loader failed, err: %v", "stats", err)
			}

			errChan <- routineResult{name: "stats-loader", err: statsLoader.Start()}
			statsLoader.Stop()
		}()

		if c.WorkType != "stats" {
			for _, t := range []string{c.WorkType, "json"} {
				totalRoutines++
				loadType := t
				load, err := core.NewLoader(loadType, &loadWg, ctx, c, sqlChan, jsonChan, statChan, flashbackChan)
				if err != nil {
					log.Logger.Error("create %s loader failed, err: %v", loadType, err)
					cancel()
					return err
				}

				go func(name string, load core.Loader) {
					errChan <- routineResult{name: name, err: load.Start()}
					load.Stop()
				}(fmt.Sprintf("loader-%s", loadType), load)
			}

			for i := range c.Threads {
				totalRoutines++
				threadNum := i
				transform := core.NewTransformer("default", &transWg, ctx, threadNum, &trCnt, c, tbColsInfo, eventChan,
					sqlChan, jsonChan, lock)

				go func(name string, transform core.Transformer) {
					errChan <- routineResult{name: name, err: transform.Start()}
					transform.Stop()
				}(fmt.Sprintf("transformer-%d", threadNum), transform)
			}
		}

		if c.FlashbackBinlog {
			totalRoutines++
			flashbackLoader, err := core.NewLoader("flashback", &loadWg, ctx, c, sqlChan, jsonChan, statChan, flashbackChan)
			if err != nil {
				log.Logger.Error("create %s loader failed, err: %v", "flashback", err)
				cancel()
				return err
			}

			go func() {
				errChan <- routineResult{name: "loader-flashback", err: flashbackLoader.Start()}
				flashbackLoader.Stop()
			}()
		}

		totalRoutines++
		extract := core.NewExtractor(c.Mode, &extractWg, ctx, c, eventChan, statChan, flashbackChan)
		go func() {
			errChan <- routineResult{name: "extractor", err: extract.Start()}
			extract.Stop()
		}()

		f := StartCpuProfile()
		defer StopCpuProfile(f)

		// finish cpu perf profiling before ctrl-C/kill/kill -15
		ch := make(chan os.Signal, 5)
		signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
		go func() {
			for {
				sig := <-ch
				switch sig {
				case syscall.SIGINT, syscall.SIGTERM:
					cancel()
					log.Logger.Debug("Terminating process, will finish cpu pprof before exit(if specified)...")
					StopCpuProfile(f)
					return
				default:
					// do nothing
				}
			}
		}()

		for finishRoutines < totalRoutines {
			res := <-errChan
			if res.err != nil {
				cancel()
				if !errors.Is(res.err, context.Canceled) && !errors.Is(res.err, vars.ManualKill) {
					if runErr == nil {
						runErr = res.err
					}
					log.Logger.Error("my2sql-plus got err from %s, err: %v", res.name, res.err)
				}
			}

			finishRoutines++
			switch {
			case res.name == "extractor":
				if c.WorkType != "stats" && !eventChanClosed {
					close(eventChan)
					eventChanClosed = true
				}
				if !statChanClosed {
					close(statChan)
					statChanClosed = true
				}
				if c.FlashbackBinlog && !flashbackClosed {
					close(flashbackChan)
					flashbackClosed = true
				}
			case len(res.name) >= len("transformer-") &&
				res.name[:len("transformer-")] == "transformer-":
				transformDone++
				if c.WorkType != "stats" && transformDone == c.Threads && !sqlChanClosed {
					close(sqlChan)
					close(jsonChan)
					sqlChanClosed = true
				}
			}
		}

		cancel()
		extractWg.Wait()
		transWg.Wait()
		loadWg.Wait()

		// do memory profiling before exit
		MemProfile()
		return runErr
	},
}

func StartCpuProfile() *os.File {
	if cpuprofile != "" {
		f, err := os.Create(cpuprofile)
		if err != nil {
			log.Logger.Fatal("could not create CPU profile: %v", err)
		}
		if err = pprof.StartCPUProfile(f); err != nil {
			log.Logger.Fatal("could not start CPU profile: %v", err)
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
			log.Logger.Fatal("could not create memory profile: %v", err)
		}
		defer f.Close()
		runtime.GC() // get up-to-date statistics
		if err = pprof.WriteHeapProfile(f); err != nil {
			log.Logger.Fatal("could not write memory profile: %v", err)
		}
		log.Logger.Info("mem pprof done [file=%s]!", memprofile)
	}
}

func initRun() {
	runCmd.Flags().StringVar(&cpuprofile, "cpuprofile", "", "Write CPU profile data to `file`.")
	runCmd.Flags().StringVar(&memprofile, "memprofile", "", "Write memory profile data to `file`.")

	runCmd.Flags().StringVar(&c.Mode, "mode", "repl", utils.SliceToString(vars.GOptsValidMode, vars.JoinSepComma, vars.ValidOptMsg)+". repl reads binlog from MySQL as a replica; file reads a local binlog file. Default: repl.")
	runCmd.Flags().StringVar(&c.WorkType, "work-type", "2sql", utils.SliceToString(vars.GOptsValidWorkType, vars.JoinSepComma, vars.ValidOptMsg)+". 2sql generates forward SQL; rollback generates rollback SQL; stats analyzes transactions. Default: 2sql.")
	runCmd.Flags().StringVar(&c.MySQLType, "mysql-type", "mysql", utils.SliceToString(vars.GOptsValidDBType, vars.JoinSepComma, vars.ValidOptMsg)+". Source database flavor. Default: mysql.")

	runCmd.Flags().StringVar(&c.Host, "host", "127.0.0.1", "MySQL host for binlog access and schema lookup. Default: 127.0.0.1.")
	runCmd.Flags().UintVar(&c.Port, "port", 3306, "MySQL port for binlog access and schema lookup. Default: 3306.")
	runCmd.Flags().StringVar(&c.User, "user", "", "MySQL user for binlog access and schema lookup.")
	runCmd.Flags().StringVar(&c.Passwd, "password", "", "MySQL password for binlog access and schema lookup.")
	runCmd.Flags().UintVar(&c.ServerId, "server-id", 1113306, "Server ID used by repl mode when connecting as a replica. It must be unique across replicas. Default: 1113306.")

	runCmd.Flags().StringVar(&dbs, "databases", "", "Only parse these databases. Use commas to separate multiple names.")
	runCmd.Flags().StringVar(&tbs, "tables", "", "Only parse these tables. Do not include the schema name. Use commas to separate multiple names.")
	runCmd.Flags().StringVar(&ignoreDBs, "ignore-databases", "", "Skip these databases. Use commas to separate multiple names.")
	runCmd.Flags().StringVar(&ignoreTBs, "ignore-tables", "", "Skip these tables. Do not include the schema name. Use commas to separate multiple names.")
	runCmd.Flags().StringVar(&sqlTypes, "sql", "", utils.SliceToString(vars.GOptsValidFilterSQL, vars.JoinSepComma, vars.ValidOptMsg)+". Only parse these SQL types. Use commas to separate multiple values. By default all supported DML types are included.")
	runCmd.Flags().BoolVar(&c.IgnorePrimaryKeyForInsert, "ignore-primaryKey-forInsert", false, "When --work-type=2sql, omit primary-key columns from generated INSERT statements.")

	runCmd.Flags().StringVar(&c.StartFile, "start-file", "", "Start reading from this binlog file.")
	runCmd.Flags().UintVar(&c.StartPos, "start-pos", 4, "Start reading at this binlog position.")
	runCmd.Flags().StringVar(&c.StopFile, "stop-file", "", "Stop when this binlog file is reached.")
	runCmd.Flags().UintVar(&c.StopPos, "stop-pos", 4, "Stop reading at this binlog position.")
	runCmd.Flags().StringVar(&c.LocalBinFile, "local-binlog-file", "", "Local binlog file to process. Only used with --mode=file.")

	runCmd.Flags().StringVar(&c.BinlogTimeLocation, "tl", "Local", "Time zone used to parse timestamp/datetime columns from binlog, for example Asia/Shanghai. Default: Local.")
	runCmd.Flags().StringVar(&startTime, "start-datetime", "", "Start reading at the first event whose datetime is equal to or later than this value. Format: \"2020-01-01 01:00:00\".")
	runCmd.Flags().StringVar(&stopTime, "stop-datetime", "", "Stop at the first event whose datetime is equal to or later than this value. Format: \"2020-12-30 01:00:00\".")

	runCmd.Flags().BoolVar(&c.OutputToScreen, "output-toScreen", false, "Print generated SQL to stdout instead of writing SQL files.")
	runCmd.Flags().BoolVar(&c.PrintExtraInfo, "add-extraInfo", false, "When --work-type=2sql|rollback, add a metadata comment line before each SQL statement.")
	runCmd.Flags().BoolVar(&c.FlashbackBinlog, "flashback-binlog", false, "When --work-type=rollback, also generate a reverse binlog file.")
	runCmd.Flags().StringVar(&c.FlashbackBinlogBase, "flashback-binlog-base", "", "Base name for reverse binlog output. Only used with --flashback-binlog. Relative paths are resolved against --output-dir.")
	runCmd.Flags().BoolVar(&c.Summary, "summary", false, "When --work-type=rollback, print a rollback summary after parsing.")
	runCmd.Flags().StringVar(&c.SummaryFile, "summary-file", "", "Write rollback summary to file and implicitly enable --summary. Relative paths are resolved against --output-dir.")

	runCmd.Flags().BoolVar(&c.FullColumns, "full-columns", false, "For UPDATE, include unchanged columns. For UPDATE and DELETE, prefer full-row predicates instead of only primary or unique key predicates.")
	runCmd.Flags().BoolVar(&doNotAddPrefixDB, "do-not-add-prefixDb", false, "Do not prefix table names with database names in generated SQL.")
	runCmd.Flags().BoolVar(&c.UseUniqueKeyFirst, "U", false, "Prefer unique keys over primary keys when building WHERE conditions for UPDATE and DELETE.")

	runCmd.Flags().StringVar(&c.OutputDir, "output-dir", "", "Directory for generated output files. Defaults to the current working directory.")
	runCmd.Flags().BoolVar(&c.FilePerTable, "file-per-table", false, "Write one SQL/JSON file per table instead of one file per binlog.")
	runCmd.Flags().IntVar(&c.PrintInterval, "print-interval", vars.GetDefaultValueOfRange("PrintInterval"), "When --work-type=stats, print statistics every PrintInterval seconds. "+vars.GetDefaultAndRangeValueMsg("PrintInterval"))
	runCmd.Flags().IntVar(&c.BigTrxRowLimit, "big-trx-row-limit", vars.GetDefaultValueOfRange("BigTrxRowLimit"), "When --work-type=stats, treat transactions with affected rows greater than or equal to this value as big transactions. "+vars.GetDefaultAndRangeValueMsg("BigTrxRowLimit"))
	runCmd.Flags().IntVar(&c.LongTrxSeconds, "long-trx-seconds", vars.GetDefaultValueOfRange("LongTrxSeconds"), "When --work-type=stats, treat transactions with duration greater than or equal to this value as long transactions. "+vars.GetDefaultAndRangeValueMsg("LongTrxSeconds"))

	runCmd.Flags().IntVar(&c.Threads, "threads", vars.GetDefaultValueOfRange("Threads"), "When --work-type=2sql|rollback, number of worker threads to run.")
	configureRunHelp(runCmd)
	rootCmd.AddCommand(runCmd)
}
