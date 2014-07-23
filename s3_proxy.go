package main

import (
	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/s3"

	"net/http"
	"time"
	"runtime"
	"fmt"
	"os"
	"strings"
	"encoding/xml"
	"io"
//	"flag"
)

func InitS3() *s3.S3 {
	// XXX
	auth, _ := aws.EnvAuth()
	return s3.New(auth, aws.Regions[os.Getenv("AWS_REGION")])
}

func getObject(bucketName string, key string, headers http.Header) (*http.Response, error) {
	client := InitS3()
	bucket := client.Bucket(bucketName)

	resp, err := bucket.GetResponseWithHeaders(key, headers)

	return resp, err
}

func HandleRequest(writer http.ResponseWriter, request *http.Request) {
	pathes := strings.SplitN(request.URL.Path,"/",3)

	if(len(pathes) < 3) {
		http.NotFound(writer, request)
		return
	}

	resp, err:= getObject(pathes[1], pathes[2], request.Header)

	if err != nil {
		switch s3err := err.(type) {
		case *s3.Error:
			xml, xerr := xml.Marshal(s3err)
			if xerr != nil {
				// XXX
				panic(xerr.Error())
			}

			writer.Header().Set("Server", "s3_proxy")
			writer.Header().Set("Content-Type", "application/xml")
			writer.WriteHeader(s3err.StatusCode)

			fmt.Fprintf(writer, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>")
			fmt.Fprintf(writer, "<Error>")
			writer.Write(xml)
			fmt.Fprintf(writer, "</Error>")
		default:
			writer.Header().Set("Server", "s3_proxy")
			writer.WriteHeader(500)
			fmt.Fprintf(writer, "%s", err.Error())
		}
		fmt.Println(err)
		return
	}

	for name, vs := range resp.Header {
		for _, value := range vs {
			writer.Header().Add(name, value)
		}
	}
	writer.Header().Set("Server", "s3_proxy")
	writer.WriteHeader(resp.StatusCode)

	_, err = io.Copy(writer, resp.Body)
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println(request.Header)
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	serveMux := http.NewServeMux()

	serveMux.HandleFunc("/", HandleRequest)

	httpServ := &http.Server {
		Addr: ":8080",
		Handler: serveMux,
		ReadTimeout:    10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	httpServ.ListenAndServe()
}
