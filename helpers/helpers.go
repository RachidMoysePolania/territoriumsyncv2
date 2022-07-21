package helpers

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/gocarina/gocsv"
)

type Model struct {
	Id      int    `csv:"Id"`
	Url     string `csv:"Url"`
	Destino string `csv:"Destino"`
}

func ErrorLogger() *log.Logger {
	logerr, err := os.OpenFile("error.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0777)
	if err != nil {
		log.Println("Error al crear el archivo de logs")
	}
	defer logerr.Close()
	loggererror := log.New(logerr, "ERROR ", log.LstdFlags)

	return loggererror
}

func InfoLogger() *log.Logger {
	infolog, err := os.OpenFile("info.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0777)
	if err != nil {
		log.Println("Error al crear el archivo de logs")
	}
	defer infolog.Close()
	infologger := log.New(infolog, "INFO ", log.LstdFlags)

	return infologger
}

var errlog = ErrorLogger()

func DownloadFromBlobStorage(url string) ([]byte, error) {
	resp, err := http.Get(url)
	if err != nil {
		errlog.Println(fmt.Sprintf("Error al hacer la peticion: %v", err))
		log.Println(fmt.Sprintf("Error al hacer la peticion: %v", err))
		return nil, err
	}

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		errlog.Println(fmt.Sprintf("Error al leer los datos del body: %v", err))
		log.Println(fmt.Sprintf("Error al leer los datos del body: %v", err))
		return nil, err
	}

	//FIXME
	if strings.Contains(string(data), "BlobArchived") {
		errlog.Println("Archived File! skipping")
		log.Println("Archived File! skipping")
		return nil, errors.New(fmt.Sprintf("Archived File! Skipping: %v", url))
	}

	return data, nil
}

func UploadDataToS3Bucket(filename string, data []byte, bucketname string) *manager.UploadOutput {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		errlog.Println(fmt.Sprintf("Error al cargar la configuracion!: %v", err))
		log.Println(fmt.Sprintf("Error al cargar la configuracion!: %v", err))
	}

	client := s3.NewFromConfig(cfg)
	uploader := manager.NewUploader(client)
	result, err := uploader.Upload(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucketname),
		Key:    aws.String(filename),
		Body:   strings.NewReader(string(data)),
	})
	if err != nil {
		errlog.Println(fmt.Sprintf("Error al subir los archivos al bucket S3: %v", err))
		log.Println(fmt.Sprintf("Error al subir los archivos al bucket S3: %v", err))
	}
	return result
}

func ReadCSV(pathfile string) ([]Model, error) {
	csvfile, err := os.OpenFile(pathfile, os.O_RDONLY|os.O_RDWR, 0777)
	if err != nil {
		errlog.Println(err)
		return nil, err
	}
	defer csvfile.Close()
	var models []Model = []Model{}
	if err := gocsv.UnmarshalFile(csvfile, &models); err != nil {
		errlog.Println(err)
		return nil, err
	}
	return models, nil
}

func ParsingUrl(urls ...string) ([]string, error) {
	var decodedurls []string
	for _, u := range urls {
		decoded, err := url.QueryUnescape(u)
		if err != nil {
			errlog.Println("error al decodear la url")
			log.Println("error al decodear la url")
			return nil, err
		}
		decodedurls = append(decodedurls, decoded)
	}
	return decodedurls, nil
}

func GetBucketObjects(origin string, carpeta ...string) []string {
	var allobjects []string
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		errlog.Println(err)
	}

	//Creating a new S3 client
	client := s3.NewFromConfig(cfg)

	//Get firstPage of results
	for _, f := range carpeta {
		output, err := client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
			Bucket: aws.String(fmt.Sprintf("%v", origin)),
			Prefix: aws.String(fmt.Sprintf("%v", f)),
		})
		if err != nil {
			errlog.Println(err)
		}
		for _, object := range output.Contents {
			allobjects = append(allobjects, aws.ToString(object.Key))
		}
	}

	return allobjects
}

func DownloadFilesFromBucket(bucket string, files ...string) ([]byte, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		errlog.Fatalln(err)
	}
	//Creating a new S3 client
	client := s3.NewFromConfig(cfg)

	buffer := manager.NewWriteAtBuffer([]byte{})
	downloader := manager.NewDownloader(client)

	for _, file := range files {
		numbytes, err := downloader.Download(context.TODO(), buffer, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(file),
		})
		if err != nil {
			errlog.Println(fmt.Sprintf("Error al descargar los archivos!: %v", err))
			log.Println(fmt.Sprintf("Error al descargar los archivos!: %v", err))
			return nil, err
		}
		if numbytes < 1 {
			return nil, errors.New("Zero bytes written to memory")
		}
	}
	return buffer.Bytes(), nil
}

func DivideFileByParts(models []Model) []map[string]string {
	var tmpmap map[string]string = map[string]string{}
	var finalmap []map[string]string = []map[string]string{}
	counter := 0
	for _, model := range models {
		tmpmap[model.Url] = model.Destino
		if counter == 50000 {
			finalmap = append(finalmap, tmpmap)
			counter = 0
		}
		counter += 1
	}
	return finalmap
}
