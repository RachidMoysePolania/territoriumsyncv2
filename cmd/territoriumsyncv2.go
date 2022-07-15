package cmd

import (
	"github.com/RachidMoysePolania/territoriumsyncv2/modules"
	"github.com/spf13/cobra"
)

var path string
var downloadtype string
var bucketname string
var territoriumsync = &cobra.Command{
	Use:   "territoriumsyncv2",
	Short: "test",
	Long:  "test2",
	Run: func(cmd *cobra.Command, args []string) {
		modules.DownloadFromBlobStorage(path, downloadtype, bucketname)
	},
}

func init() {
	rootCmd.AddCommand(territoriumsync)
	territoriumsync.Flags().StringVarP(&path, "pathfile", "p", "", "Define the pathfile of csv")
	territoriumsync.Flags().StringVarP(&downloadtype, "downloadtype", "d", "local", "define the download type local or uploadtoaws")
	territoriumsync.Flags().StringVarP(&bucketname, "bucketname", "b", "", "define the bucket to upload the data")

}
