package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/SisyphusSQ/my2sql/internal/log"
	"github.com/SisyphusSQ/my2sql/internal/vars"
)

var rootCmd = &cobra.Command{
	Use:  vars.AppName,
	Long: fmt.Sprintf("%s", vars.AppName),
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("Option missed! Use %s -h or --help for details.\n", vars.AppName)
	},
}

func initAll() {
	initVersion()
	initRun()
}

func Execute() {
	initAll()
	if err := rootCmd.Execute(); err != nil {
		log.Logger.Error("my2sql execute got err: %v", err)
		os.Exit(1)
	}
}
