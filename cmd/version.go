package cmd

import (
	"fmt"

	"github.com/fatih/color"
	"github.com/spf13/cobra"

	"github.com/SisyphusSQ/my2sql/internal/vars"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: fmt.Sprintf("Show version of %s", vars.AppName),
	Long:  fmt.Sprintf("Show version of %s", vars.AppName),
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println(color.CyanString("AppVersion: "), vars.AppVersion)
		fmt.Println(color.CyanString("Go Version: "), vars.GoVersion)
		fmt.Println(color.CyanString("Build Time: "), vars.BuildTime)
		fmt.Println(color.CyanString("Git Commit: "), vars.GitCommit)
		fmt.Println(color.CyanString("Git Remote: "), vars.GitRemote)
	},
}

func initVersion() {
	rootCmd.AddCommand(versionCmd)
}
