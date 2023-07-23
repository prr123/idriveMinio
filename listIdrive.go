// listIdrive.go
// program that lists all Objects of a bucket
// Author: prr, azul software
// Date 21 July 2023
// copyright 2023 prr, azul software
//

package main

import (
    "fmt"
    "log"
    "context"
	"os"
//	"time"
//	"strings"

	minioLib "api/idriveMinio/minioLib"
    idrive "api/idriveMinio/idriveLib"

    util "github.com/prr123/utility/utilLib"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func main() {

	numArgs := len(os.Args)

	useStr := "listIdrive [/bucket=bucket] [/obj=obj] [/dbg]"
	helpStr := "program that list all buckets and objects"

	flags:=[]string{"bucket","obj","dbg"}
	dbg:=false

	if numArgs < 2 {
		fmt.Printf("usage: %s\n", useStr)
		log.Fatalf("insufficient args!")
	}

	if numArgs > len(flags) + 1  {
		fmt.Printf("usage: %s\n", useStr)
		log.Fatalf("too many args!")
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

	destBucket := "*"
	buckval, ok := flagMap["bucket"]
	if !ok  {
		fmt.Printf("no bucket flag! bucket flag is required!")
        fmt.Printf("usage is: %s\n", useStr)
        os.Exit(-1)
	}
	if buckval.(string) == "none" || buckval.(string) == "all" {
		destBucket = "*"
	} else {
		destBucket = buckval.(string)
	}

	if dbg {
		log.Printf("destination Bucket: %s\n", destBucket)
	}

	bL := []string{}
	buckList := &bL
	if destBucket != "*" {
		buckList, err = util.ParseList(destBucket)
		if err != nil {log.Fatalf("ParseList %v",err)}
		if dbg {util.PrintList(buckList)}
	}


	dispObj := 0
	objval, ok := flagMap["obj"]
	destObj := ""
	if ok  {
		if objval.(string) != "none" {
			destObj = objval.(string)
			dispObj = 2
		} else {
			destObj = "all"
			dispObj = 1
		}
	}

	if dbg {
		log.Printf("dispObj: %d\n", dispObj)
		log.Printf("destination Objects: %s\n", destObj)
	}

	if dispObj > 0 {
		objList, err := util.ParseList(destObj)
		if err != nil {log.Fatalf("ParseList %v",err)}
		if dbg {util.PrintList(objList)}
	}

    api, err := idrive.GetIdriveApi("idriveApi.yaml")
    if err != nil {log.Fatalf("getIdriveApi: %v\n", err)}
    if dbg {log.Println("success idrive api")}

    secret, err := idrive.GetSecret()
    if err != nil {log.Fatalf("getSecret: %v\n", err)}
//    log.Printf("secret: %s", secret)

    api.Secret = secret

	if dbg {idrive.PrintApiObj(api)}


	// Initialize minio client object.
	minioClient, err := minio.New(api.Url, &minio.Options{
		Creds:  credentials.NewStaticV4(api.Key, secret, ""),
		Secure: true,
	})
	if err != nil {
		log.Fatalf("minio New Client: %v",err)
	}

	if dbg {log.Printf("client generated!")}
//	log.Printf("client:\n%#v\n", minioClient) // minioClient is now set up

//	minioClient.TraceOn(os.Stderr)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	buckets, err := minioClient.ListBuckets(ctx)
	if err != nil {
    	log.Fatalf("minio ListBuckets: %v", err)
	}
	if dbg {minioLib.PrintBuckets(buckets)}


	if destBucket == "*" {
		bL:= make([]string, len(buckets))
		for i:=0; i<len(buckets); i++ {
			bL[i] = buckets[i].Name
		}
		buckList = &bL
	} else {
		err = minioLib.FindBuckets(buckList, buckets)
		if err != nil {log.Fatalf("no match between cli bucketlist! %v", err)}
	}
	fmt.Printf("****** Bucket List[%d] ********\n", len(*buckList))
	util.PrintList(buckList)
	fmt.Printf("****** End Bucket List ********\n")

	log.Printf("all cli bucket names found in retrieved bucketlist!\n")
	if dispObj == 0 {os.Exit(1)}

	opt := minio.ListObjectsOptions{
		Recursive: true,
	}

	for i:=0; i< len(*buckList); i++ {
		log.Printf("objects for bucket '%s'\n")
		objChan := minioClient.ListObjects(ctx, (*buckList)[i], opt)
		if err != nil {log.Fatalf("ListObjects of bucket %s: %v", (*buckList)[i], err)}

		fmt.Printf("********* objects for bucket %s: %d ***********\n", (*buckList)[i], len(objChan))
		count:=0
//		obj1 := minio.ObjectInfo{}
		for obj := range objChan {
			if obj.Err != nil {
				fmt.Printf("object error: %v\n", obj.Err)
			}
			fmt.Printf("Object[%d]: \n", count+1)
			minioLib.PrintObjInfo(&obj)
			count++
		}
	}

/*
	// metadata
			fmt.Println("************ Object Metadata *************")
			objInfo, err := minioClient.StatObject(ctx, (*buckList)[i], obj.Key, minio.StatObjectOptions{})
			if err != nil {log.Fatalf("StatObject: %v", err)}
			PrintObjInfo(&objInfo)

*/
	log.Println("Successfully listed objects!")

}

