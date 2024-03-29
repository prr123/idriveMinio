// upload.go
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
	"os"
  	"time"
//	"strings"

    util "github.com/prr123/utility/utilLib"
    idrive "api/idriveMinio/idriveLib"
    minioLib "api/idriveMinio/minioLib"


	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func main() {

	numArgs := len(os.Args)

	useStr := "upload [/file=file] [/bucket=bucket] [/db]"
	helpStr := "program that uploads a file\n  requires: /file and /bucket flags\n"

	flags := []string{"file", "bucket", "dbg"}
	dbg := false

	if numArgs <2  {
		fmt.Printf("usage: %s\n", useStr)
		log.Fatalf("insufficient args!")
	}

	if numArgs > len(flags) + 1 {
		fmt.Printf("usage: %s\n", useStr)
		log.Fatalf("too manu#y args!")
	}

   if numArgs > 1 && os.Args[1] == "help" {
        fmt.Printf("help: %s\n", helpStr)
        fmt.Printf("usage is: %s\n", useStr)
        os.Exit(-1)
    }

    flagMap, err := util.ParseFlags(os.Args, flags)
    if err != nil {log.Fatalf("util.ParseFlags: %v\n", err)}

    _, ok := flagMap["dbg"]
    if ok {dbg = true}
    if dbg {
        for k, v :=range flagMap {
            fmt.Printf("flag: /%s value: %s\n", k, v)
        }
    }

	destBucket :=""
    buckval, ok := flagMap["bucket"]
    if !ok  {
        fmt.Printf("no bucket flag! bucket flag is required!")
        fmt.Printf("usage is: %s\n", useStr)
        os.Exit(-1)
    }
    if buckval.(string) == "none" {
        fmt.Printf("no buckets listed! bucket flag requires value!")
        fmt.Printf("usage is: %s\n", useStr)
        os.Exit(-1)
    }
	if buckval.(string) == "all" || buckval.(string) == "*" {
        fmt.Printf("error: /bucket value cannot be 'all' or '*'!")
        fmt.Printf("usage is: %s\n", useStr)
        os.Exit(-1)
	}

	destBucket = buckval.(string)

    buckList, err := util.ParseList(destBucket)
    if err != nil {log.Fatalf("Buckets ParseList %v",err)}

	if len(*buckList) > 1 {
		log.Fatalf("error: /bucket flag has more than one bucket!")
	}
	if dbg {
		fmt.Printf("*** Buckets: *****\n")
		util.PrintList(buckList)
	}

	upFilnams:=""
	multiFiles := false
    fileval, ok := flagMap["file"]
    if !ok  {
        fmt.Printf("error: no file flag! file flag is required!")
        fmt.Printf("usage is: %s\n", useStr)
        os.Exit(-1)
    }
    if fileval.(string) == "none" {
        fmt.Printf("error: /file flag has no files listed! file flag requires value!")
        fmt.Printf("usage is: %s\n", useStr)
        os.Exit(-1)
    }
	if fileval.(string) == "all" || fileval.(string) == "*" {
		multiFiles = true
	}

	upFilnams = fileval.(string)

	var filNamList *[]string
    filNamList, err = util.ParseList(upFilnams)
    if err != nil {log.Fatalf("upFiles ParseList %v",err)}

	if dbg {
		fmt.Printf("*** Files: *****\n")
		util.PrintList(filNamList)
	}

	objB := []byte(os.Args[1])
	for i:=0; i< len(objB); i++ {
		if objB[i] == '.' {objB[i] = '_'}
	}
	objNam := string(objB)
	srcFilnam := "testData/"

/*
	log.Printf("source file: %s\n", srcFilnam)
	log.Printf("objNam: %s\n", objNam)
	log.Printf("destination Bucket: %s\n", destBucket)
*/

    api, err := idrive.GetIdriveApi("idriveApi.yaml")
    if err != nil {log.Fatalf("getIdriveApi: %v\n", err)}

    secret, err := idrive.GetSecret()
    if err != nil {log.Fatalf("getSecret: %v\n", err)}
//    log.Printf("secret: %s", secret)

    api.Secret = secret

    idrive.PrintApiObj(api)

	// Initialize minio client object.
	minioClient, err := minio.New(api.Url, &minio.Options{
		Creds:  credentials.NewStaticV4(api.Key, secret, ""),
		Secure: true,
	})
	if err != nil {
		log.Fatalf("error: could not create minio client: %v",err)
	}

    if dbg {log.Println("success creating minio client!")}


//	minioClient.TraceOn(os.Stderr)
	ctx := context.Background()
	buckets, err := minioClient.ListBuckets(ctx)
	if err != nil {
    	log.Fatalf("minio ListBuckets: %v", err)
	}

	// test buckets
	err = minioLib.FindBuckets(buckList, buckets)
	if err != nil {log.Fatalf("no match between cli and bucketlist! %v", err)}
	log.Printf("buckets found!\n")

	// test files


	if multiFiles {
		filNames := make([]string, 10)
		files, err := os.ReadDir("testData/")
		if err != nil {log.Fatalf("readDir 'testData': %v",err)}

		count:=0
		for _, file := range files {
			if dbg {fmt.Println(file.Name(), file.IsDir())}
			if file.IsDir() {continue}
			filNames[count] = file.Name()
			count++
			if count > 9 {log.Fatalf("too many files!")}
		}
		lp:= filNames[:count]
		filNamList = &lp

	}

	for i:=0; i<len(*filNamList); i++ {
		filnam := "testData/" + (*filNamList)[i]
		info, err := os.Stat(filnam)
		if err != nil {
			log.Printf("file %s does not exist: %v\n", filnam, err)
			continue
		}
		log.Printf("file: %s size: %d\n", filnam, info.Size())
	}


	os.Exit(1)

	srcFil, err := os.Open(srcFilnam)
	if err != nil {log.Fatalf("os.Open:%v", err)}
	defer srcFil.Close()


	fileStat, err :=  srcFil.Stat()
	if err != nil {log.Fatalf("file.Stat: %v", err)}
	log.Printf("fils size: %d\n", fileStat.Size())

	opt := minio.PutObjectOptions{ContentType:"application/octet-stream"}

	uploadInfo, err := minioClient.PutObject(ctx, destBucket, objNam, srcFil, fileStat.Size(), opt)
	if err != nil {log.Fatalf("PutObject: %v", err)}

	PrintUploadInfo(&uploadInfo)
	log.Println("Successfully uploaded file")

}

func PrintUploadInfo(info *minio.UploadInfo) {
	fmt.Printf("*** upload info ***\n")
	fmt.Printf("Bucket: %s\n", info.Bucket)
	fmt.Printf("Etag: %s\n", info.ETag)
	fmt.Printf("Size: %d\n", info.Size)
	fmt.Printf("Mod:  %s\n", info.LastModified.Format(time.RFC1123))
	fmt.Printf("Version ID: %s\n", info.VersionID)
}
