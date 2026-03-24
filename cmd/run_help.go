package cmd

import (
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"
	flag "github.com/spf13/pflag"
)

type helpFlagGroup struct {
	title string
	flags []string
}

const runHelpShort = "Parse MySQL row binlog and generate SQL, JSON, rollback, or stats output."

var runHelpDescription = strings.TrimSpace(`
my2sql-plus reads MySQL row binlog from a live replication stream or a local
binlog file and generates forward SQL, rollback SQL, JSON rows, or transaction
statistics for row-based DML events.
`)

var runHelpNotes = []string{
	"Only row-based DML binlog parsing is a supported primary use case.",
	"repl mode connects to MySQL as a replica and requires a unique --server-id.",
	"file mode still queries MySQL for table metadata; it is not a fully offline SQL generator.",
	"Both 2sql and rollback currently produce JSON output in addition to SQL output.",
	"Rollback-only options such as --flashback-binlog and --summary work only with --work-type=rollback.",
}

var runHelpExamples = strings.TrimSpace(`
1) Generate forward SQL from a local binlog file:
   ./bin/my2sql run \
     --mode=file \
     --work-type=2sql \
     --host=127.0.0.1 \
     --port=3306 \
     --user=root \
     --password=secret \
     --local-binlog-file=/path/to/mysql-bin.000001

2) Generate forward SQL in repl mode from a given binlog position:
   ./bin/my2sql run \
     --mode=repl \
     --work-type=2sql \
     --host=127.0.0.1 \
     --port=3306 \
     --user=root \
     --password=secret \
     --server-id=223306 \
     --start-file=mysql-bin.000001 \
     --start-pos=4

3) Parse a datetime range from a local binlog file:
   ./bin/my2sql run \
     --mode=file \
     --work-type=2sql \
     --host=127.0.0.1 \
     --port=3306 \
     --user=root \
     --password=secret \
     --local-binlog-file=/path/to/mysql-bin.000001 \
     --start-datetime="2024-01-01 00:00:00" \
     --stop-datetime="2024-01-01 01:00:00"

4) Filter by database, table, and SQL type:
   ./bin/my2sql run \
     --mode=file \
     --work-type=2sql \
     --host=127.0.0.1 \
     --port=3306 \
     --user=root \
     --password=secret \
     --local-binlog-file=/path/to/mysql-bin.000001 \
     --databases=db1 \
     --tables=orders \
     --sql=insert,update

5) Generate rollback SQL, reverse binlog, and rollback summary:
   ./bin/my2sql run \
     --mode=file \
     --work-type=rollback \
     --host=127.0.0.1 \
     --port=3306 \
     --user=root \
     --password=secret \
     --local-binlog-file=/path/to/mysql-bin.000001 \
     --flashback-binlog \
     --flashback-binlog-base=rollback_out \
     --summary \
     --summary-file=rollback-summary.txt

6) Analyze transactions in stats mode:
   ./bin/my2sql run \
     --mode=file \
     --work-type=stats \
     --host=127.0.0.1 \
     --port=3306 \
     --user=root \
     --password=secret \
     --local-binlog-file=/path/to/mysql-bin.000001 \
     --output-dir=/tmp/my2sql-plus-out

7) Split SQL output by table and add metadata comments:
   ./bin/my2sql run \
     --mode=file \
     --work-type=2sql \
     --host=127.0.0.1 \
     --port=3306 \
     --user=root \
     --password=secret \
     --local-binlog-file=/path/to/mysql-bin.000001 \
     --file-per-table \
     --add-extraInfo
`)

var runHelpFlagGroups = []helpFlagGroup{
	{
		title: "Input and source",
		flags: []string{
			"mode",
			"host",
			"port",
			"user",
			"password",
			"mysql-type",
			"server-id",
			"local-binlog-file",
		},
	},
	{
		title: "Filters",
		flags: []string{
			"databases",
			"tables",
			"ignore-databases",
			"ignore-tables",
			"sql",
		},
	},
	{
		title: "Time and positions",
		flags: []string{
			"start-file",
			"start-pos",
			"stop-file",
			"stop-pos",
			"start-datetime",
			"stop-datetime",
			"tl",
		},
	},
	{
		title: "SQL generation behavior",
		flags: []string{
			"work-type",
			"ignore-primaryKey-forInsert",
			"full-columns",
			"do-not-add-prefixDb",
			"U",
			"threads",
		},
	},
	{
		title: "Output and files",
		flags: []string{
			"output-dir",
			"file-per-table",
			"output-toScreen",
			"add-extraInfo",
			"cpuprofile",
			"memprofile",
		},
	},
	{
		title: "Rollback-specific options",
		flags: []string{
			"flashback-binlog",
			"flashback-binlog-base",
			"summary",
			"summary-file",
		},
	},
	{
		title: "Stats-specific options",
		flags: []string{
			"print-interval",
			"big-trx-row-limit",
			"long-trx-seconds",
		},
	},
}

func configureRunHelp(cmd *cobra.Command) {
	cmd.Short = runHelpShort
	cmd.Long = runHelpDescription
	cmd.Example = runHelpExamples
	cmd.InitDefaultHelpFlag()
	if helpFlag := cmd.Flags().Lookup("help"); helpFlag != nil {
		helpFlag.Usage = "Show help for the run command."
	}
	cmd.SetHelpFunc(func(cmd *cobra.Command, _ []string) {
		writeRunHelp(cmd.OutOrStdout(), cmd)
	})
}

func writeRunHelp(w io.Writer, cmd *cobra.Command) {
	fmt.Fprintln(w, cmd.Short)
	fmt.Fprintln(w)

	fmt.Fprintln(w, "Usage:")
	fmt.Fprintf(w, "  %s\n", cmd.UseLine())
	fmt.Fprintln(w)

	fmt.Fprintln(w, "Description:")
	writeIndentedBlock(w, cmd.Long)
	fmt.Fprintln(w)

	fmt.Fprintln(w, "Key Notes:")
	for _, note := range runHelpNotes {
		fmt.Fprintf(w, "  - %s\n", note)
	}
	fmt.Fprintln(w)

	fmt.Fprintln(w, "Examples:")
	writeIndentedBlock(w, cmd.Example)
	fmt.Fprintln(w)

	for _, group := range runHelpFlagGroups {
		usage := groupedFlagUsages(cmd.Flags(), group.flags)
		if usage == "" {
			continue
		}

		fmt.Fprintf(w, "%s:\n", group.title)
		fmt.Fprintln(w, usage)
		fmt.Fprintln(w)
	}

	fmt.Fprintln(w, "Help Options:")
	fmt.Fprintln(w, "  -h, --help   Show help for the run command.")
	fmt.Fprintln(w)

	fmt.Fprintln(w, "More:")
	fmt.Fprintln(w, "  See README.md for output naming rules, caveats, and additional examples.")
}

func writeIndentedBlock(w io.Writer, content string) {
	for _, line := range strings.Split(strings.TrimSpace(content), "\n") {
		if strings.TrimSpace(line) == "" {
			fmt.Fprintln(w)
			continue
		}

		fmt.Fprintf(w, "  %s\n", line)
	}
}

func groupedFlagUsages(source *flag.FlagSet, names []string) string {
	group := flag.NewFlagSet("run-help-group", flag.ContinueOnError)
	group.SortFlags = false
	group.SetOutput(io.Discard)

	for _, name := range names {
		item := source.Lookup(name)
		if item == nil {
			continue
		}
		group.AddFlag(item)
	}

	return strings.TrimRight(group.FlagUsagesWrapped(0), "\n")
}
