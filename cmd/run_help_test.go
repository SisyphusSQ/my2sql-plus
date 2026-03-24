package cmd

import (
	"bytes"
	"strings"
	"sync"
	"testing"

	flag "github.com/spf13/pflag"
)

var initCommandsOnce sync.Once

func initCommandsForTest() {
	initCommandsOnce.Do(func() {
		initAll()
	})
}

func TestRunHelpOutputIncludesSectionsAndExamples(t *testing.T) {
	initCommandsForTest()

	buf := new(bytes.Buffer)
	runCmd.SetOut(buf)
	runCmd.SetErr(buf)

	if err := runCmd.Help(); err != nil {
		t.Fatalf("runCmd.Help() returned error: %v", err)
	}

	output := buf.String()
	for _, want := range []string{
		runHelpShort,
		"Description:",
		"Key Notes:",
		"Examples:",
		"Input and source:",
		"Rollback-specific options:",
		"Stats-specific options:",
		"./bin/my2sql run \\",
		"Show help for the run command.",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("help output is missing %q\nfull output:\n%s", want, output)
		}
	}
}

func TestRunHelpGroupsCoverAllVisibleFlags(t *testing.T) {
	initCommandsForTest()

	grouped := make(map[string]struct{})
	for _, group := range runHelpFlagGroups {
		for _, name := range group.flags {
			if _, exists := grouped[name]; exists {
				t.Fatalf("flag %q is assigned to multiple help groups", name)
			}
			grouped[name] = struct{}{}
		}
	}

	runCmd.Flags().VisitAll(func(itemFlag *flag.Flag) {
		if itemFlag.Hidden || itemFlag.Name == "help" {
			return
		}

		if _, ok := grouped[itemFlag.Name]; !ok {
			t.Fatalf("flag %q is missing from help groups", itemFlag.Name)
		}
	})
}
