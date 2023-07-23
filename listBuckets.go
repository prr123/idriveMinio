// idriveListBuckets.go
// program that lists all Buckets of the idrive account
// Author: prr, azul software
// Date 29 May 2023
// copyright 2023 prr, azul software
//

package main

import (
    "fmt"
    "log"
    "context"
//	"os"
  "time"

    idrive "api/idriveMinio/idriveLib"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func main() {

    api, err := idrive.GetIdriveApi("idriveApi.yaml")
    if err != nil {log.Fatalf("getIdriveApi: %v\n", err)}
    log.Println("success idrive api")

    secret, err := idrive.GetSecret()
    if err != nil {log.Fatalf("getSecret: %v\n", err)}
    log.Printf("secret: %s", secret)

    api.Secret = secret

    idrive.PrintApiObj(api)

//	endpoint := api.Url
//	accessKeyID := api.Key
//	secretAccessKey := secret
//	useSSL := true

	// Initialize minio client object.
	minioClient, err := minio.New(api.Url, &minio.Options{
		Creds:  credentials.NewStaticV4(api.Key, secret, ""),
		Secure: true,
	})
	if err != nil {
		log.Fatalf("minio New Client: %v",err)
	}

	log.Printf("client generated!")
//	log.Printf("client:\n%#v\n", minioClient) // minioClient is now set up

//	minioClient.TraceOn(os.Stderr)
	buckets, err := minioClient.ListBuckets(context.Background())
	if err != nil {
    	log.Fatalf("minio ListBuckets: %v", err)
	}

	fmt.Println("*********** List Buckets ***********")
	fmt.Printf("Buckets: %d\n", len(buckets))
	for _, bucket := range buckets {
		tstr := bucket.CreationDate.Format(time.RFC1123)
    	fmt.Printf("Name: %-15s Creation Date: %s\n", bucket.Name, tstr)
	}

}
