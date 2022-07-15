package modules

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/RachidMoysePolania/territoriumsyncv2/helpers"
)

var errx = helpers.ErrorLogger()
var info = helpers.InfoLogger()

func RecursiveDownloadFromS3Bucket(bucketname string, typeofdownload string, downloadpath string, prefixfolder ...string) {
	files := helpers.GetBucketObjects(bucketname, prefixfolder...)
	switch typeofdownload {
	case "local":
		os.Chdir(downloadpath)
		for _, archivo := range files {
			filename := strings.Split(archivo, "/")
			if filename[len(filename)-1] != "" {
				log.Println(fmt.Sprintf("[!] Descargando el archivo %v en la ruta %v", filename[len(filename)-1], strings.Join(filename[:len(filename)-1], "/")))
				info.Println(fmt.Sprintf("[!] Descargando el archivo %v en la ruta %v", filename[len(filename)-1], strings.Join(filename[:len(filename)-1], "/")))
				err := os.MkdirAll(strings.Join(filename[:len(filename)-1], "/"), 0755)
				if err != nil {
					errx.Fatalln("Error al crear las carpetas de destino", err)
				}
				file, err := os.Create(strings.Join(filename[:len(filename)-1], "/") + "/" + filename[len(filename)-1])
				if err != nil {
					errx.Println(err)
				}
				defer file.Close()
				//Download file
				data, err := helpers.DownloadFilesFromBucket(bucketname, archivo)
				if err != nil {
					log.Println(fmt.Sprintf("[x] Error al descargar el archivo %v, fallo con el siguiente error: %v", filename[len(filename)-1], err))
					errx.Println(fmt.Sprintf("[x] Error al descargar el archivo %v, fallo con el siguiente error: %v", filename[len(filename)-1], err))
				}

				file.Write(data)
				log.Println(fmt.Sprintf("[+] Finalizada correctamente la descarga del archivo %v", filename[len(filename)-1]))
				info.Println(fmt.Sprintf("[+] Finalizada correctamente la descarga del archivo %v", filename[len(filename)-1]))
				time.Sleep(time.Millisecond * 100)
			}
		}
	case "azure":
		//TODO Azure integration
	default:
		errx.Println("Tipo de descarga no permitida!")
		os.Exit(1)
	}
}
