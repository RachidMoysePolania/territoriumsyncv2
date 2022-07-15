package modules

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/RachidMoysePolania/territoriumsyncv2/helpers"
)

var errlog = helpers.ErrorLogger()
var infolog = helpers.InfoLogger()
var wg sync.WaitGroup

func DownloadFromBlobStorage(pathfile string, typeofdownload string, bucketname string) {
	globalstart := time.Now()
	models, err := helpers.ReadCSV(pathfile)
	if err != nil {
		errlog.Println(fmt.Sprintf("Error al leer el archivo csv: %v", err))
	}
	results := helpers.DivideFileByParts(models)
	wg.Add(1)
	switch typeofdownload {
	case "local":
		go func() {
			for _, result := range results {
				for origen, destino := range result {
					destinourl, err := helpers.ParsingUrl(destino)
					if err != nil {
						errlog.Fatalln(err)
					}
					filename := strings.Split(destinourl[0], "/")
					err = os.MkdirAll(strings.Join(filename[:len(filename)-1], "/"), 0755)
					if err != nil {
						errlog.Fatalln(err)
					}
					data, err := helpers.DownloadFromBlobStorage(origen)
					if err != nil {
						errlog.Println(fmt.Sprintf("Error al descargar: %v", err))
					}
					infolog.Println(fmt.Sprintf("Downloaded file %v", destinourl))
					f, err := os.Create(strings.Join(filename[:len(filename)-1], "/") + "/" + filename[len(filename)-1])
					if err != nil {
						errlog.Fatalln(err)
					}
					defer f.Close()
					f.Write(data)
				}
			}
			wg.Done()
		}()
	case "uploadtoaws":
		go func() {
			for _, result := range results {
				for origen, destino := range result {
					start := time.Now()
					parsedurl, err := helpers.ParsingUrl(destino)
					if err != nil {
						errlog.Fatalln(err)
					}
					downloaded, err := helpers.DownloadFromBlobStorage(origen)
					result := helpers.UploadDataToS3Bucket(parsedurl[0], downloaded, bucketname)
					infolog.Println(fmt.Sprintf("Item Uploaded %v Time Elapsed: %v", result.Location, time.Since(start)))
				}
			}
			wg.Done()
		}()
	default:
		infolog.Println("Tipo de descarga no correcto!")
		os.Exit(1)
	}
	infolog.Println(fmt.Sprintf("Task Completed %v", time.Since(globalstart)))
	wg.Wait()
}
