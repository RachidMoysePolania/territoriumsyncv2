package cmd

import (
	"github.com/RachidMoysePolania/territoriumsyncv2/modules"
	"github.com/spf13/cobra"
)

var territoriumsync = &cobra.Command{
	Use:   "territoriumsyncv2",
	Short: "test",
	Long:  "test2",
	Run: func(cmd *cobra.Command, args []string) {
		modules.DownloadFromBlobStorage("/Users/r4st4m4n/Downloads/pilot_sena_jul_8.csv", "local", "pruebas-devops-2022")
	},
}

func init() {
	rootCmd.AddCommand(territoriumsync)

}
