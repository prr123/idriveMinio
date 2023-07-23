// downloadObj.go
// program that lists all Buckets of the idrive account
// Author: prr, azul software
// Date 23 July 2023
// copyright 2023 prr, azul software
//

package main

import (
    "fmt"
    "log"
    "context"
	"os"
	"io"
//  	"time"
//	"strings"

    util "github.com/prr123/utility/utilLib"
    idrive "api/idriveMinio/idriveLib"
    minioLib "api/idriveMinio/minioLib"


	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func main() {

	numArgs := len(os.Args)

	useStr := "downloadObj [/obj=objname] [/bucket=bucket] [/file=filenam] [/db]"
	helpStr := "program that removes an object from a bucket\n  requires: /obj and /bucket flags\n"

	flags := []string{"obj", "bucket", "file","dbg"}
	dbg := false
	tgtFolder := "testDl/"

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

    tgtFilnam:=""
//  multiFiles := false
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
        fmt.Printf("error: /file flag has a value of all! Only one file is allowed!")
        fmt.Printf("usage is: %s\n", useStr)
        os.Exit(-1)
    }

    tgtFilnam = fileval.(string)
    var filNamList *[]string
    filNamList, err = util.ParseList(tgtFilnam)
    if err != nil {log.Fatalf("target Files ParseList %v",err)}
    if len(*filNamList) > 1 {
        fmt.Printf("error: /file flag has as value multiple files! Only one file is allowed!")
        fmt.Printf("usage is: %s\n", useStr)
        os.Exit(-1)
    }

	objNames:=""
//	multiFiles := false
    objval, ok := flagMap["obj"]
    if !ok  {
        fmt.Printf("error: no file flag! file flag is required!")
        fmt.Printf("usage is: %s\n", useStr)
        os.Exit(-1)
    }
    if objval.(string) == "none" {
        fmt.Printf("error: /obj flag has no objects listed! obj flag requires a value!")
        fmt.Printf("usage is: %s\n", useStr)
        os.Exit(-1)
    }
	if objval.(string) == "all" || objval.(string) == "*" {
        fmt.Printf("error: /obj flag has a value of all! Only one obj is allowed!")
        fmt.Printf("usage is: %s\n", useStr)
        os.Exit(-1)
	}

	objNames = objval.(string)

	var objList *[]string
    objList, err = util.ParseList(objNames)
    if err != nil {log.Fatalf("objNames ParseList %v",err)}
	if len(*objList) > 1 {
        fmt.Printf("error: /obj flag has as value multiple objects! Only one object is allowed!")
        fmt.Printf("usage is: %s\n", useStr)
        os.Exit(-1)
	}

//	if multiFiles {log.Fatalf("multiple Files are not allowed!")}

	if dbg {
		fmt.Printf("*** Files: *****\n")
		util.PrintList(objList)
	}

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

	tgtFilnam = tgtFolder + (*filNamList)[0]
	info, err := os.Stat(tgtFilnam)
	if err != nil {
		log.Fatalf("upload file %s does not exist: %v\n", tgtFilnam, err)
	}
	log.Printf("upload file: %s size: %d\n", tgtFilnam, info.Size())

//	os.Exit(1)

	tgtFil, err := os.Create(tgtFilnam)
	if err != nil {log.Fatalf("os.Create:%v", err)}
	defer tgtFil.Close()

/*
	fileStat, err :=  tgtFil.Stat()
	if err != nil {log.Fatalf("file.Stat: %v", err)}
	log.Printf("fils size: %d\n", fileStat.Size())
*/
	opt := minio.GetObjectOptions{}

	objNam :=  (*objList)[0]

	object, err := minioClient.GetObject(ctx, destBucket, objNam, opt)
	if err != nil {log.Fatalf("GetObject: %v", err)}
	defer object.Close()

//	minioLib.PrintDownloadInfo(&downloadInfo)
	_, err = io.Copy(tgtFil, object)
	if err != nil {
    	log.Fatalf("io.Copy: %v\n", err)
	}

	log.Println("Successfully downloaded object")

}
