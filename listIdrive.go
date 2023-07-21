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
	"time"
//	"strings"

    util "github.com/prr123/utility/utilLib"
    idrive "api/idriveMinio/idriveLib"

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

	destBucket := buckval.(string)

	if dbg {
		log.Printf("destination Bucket: %s\n", destBucket)
	}

	buckList, err := ParseList(destBucket)
	if err != nil {log.Fatalf("ParseList %v",err)}

	if dbg {PrintList(buckList)}

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
		objList, err := ParseList(destObj)
		if err != nil {log.Fatalf("ParseList %v",err)}
		if dbg {PrintList(objList)}
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
	if dbg {PrintBuckets(buckets)}

	if destBucket != "*" || destBucket != "all" {
		err = FindBuckets(buckList, buckets)
		if err != nil {log.Fatalf("no match between cli bucketlist! %v", err)}
	}

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
			PrintObjInfo(&obj)
			count++
//		if count == 1 {obj1 = obj}

	// metadata
			fmt.Println("************ Object Metadata *************")
			objInfo, err := minioClient.StatObject(ctx, (*buckList)[i], obj.Key, minio.StatObjectOptions{})
			if err != nil {log.Fatalf("StatObject: %v", err)}
			PrintObjInfo(&objInfo)
		}
	}
	log.Println("Successfully listed objects!")

}

func PrintObjInfo(info *minio.ObjectInfo) {
	fmt.Printf("******** object info **********\n")
//	fmt.Printf("Bucket: %s\n", info.Bucket)
	fmt.Printf("Version ID: %s\n", info.VersionID)
	fmt.Printf("Key: %s\n", info.Key)
	fmt.Printf("Etag: %s\n", info.ETag)
	fmt.Printf("Size: %d\n", info.Size)
	fmt.Printf("Mod:  %s\n", info.LastModified.Format(time.RFC1123))
	fmt.Printf("Exp:  %s\n", info.Expires.Format(time.RFC1123))
	fmt.Printf("UserMetaData[%d]:\n", len(info.UserMetadata))
	for key, val := range info.UserMetadata {
		fmt.Printf("  key: %s val: %s\n", key, val)
	}
	owner := info.Owner
	fmt.Printf(" XMLName: %v DisplayName: %s ID %s\n", owner.XMLName, owner.DisplayName, owner.ID)
}

func PrintBuckets(buckets []minio.BucketInfo) {
	fmt.Println("*********** List Buckets ***********")
	fmt.Printf("Buckets: %d\n", len(buckets))
//	found := false
	for _, bucket := range buckets {
		tstr := bucket.CreationDate.Format(time.RFC1123)
    	fmt.Printf("Name: %-15s Creation Date: %s\n", bucket.Name, tstr)
//		idx := strings.Index(bucket.Name, destBucket)
//		if idx > -1 {found = true; break;}
	}
//	if !found { log.Fatalf("destBucket is not in  bucket list!\n")}
	fmt.Println("********* End List Buckets *********")

}

func ParseList(src string)(dest *[]string, err error) {

	var list [10]string
	count:= 0
	stPos:=0
	for i:=0; i< len(src); i++ {
		if src[i] == ',' {
			list[count] = string(src[stPos:i])
			stPos = i+1
			count++
		}
	}

	list[count] = string(src[stPos:])
	count++
	lp := list[:count]

	return &lp, nil
}

func PrintList (list *[]string) {

	fmt.Printf("items: %d\n", len(*list))
	for i:=0; i< len(*list); i++ {
		fmt.Printf("%d: %s\n", i+1, (*list)[i])
	}

}

func FindBuckets(tgtList *[]string, srcList []minio.BucketInfo) (err error){

	for i:=0; i< len(*tgtList); i++ {
		tgtNam := (*tgtList)[i]
		found := false
		for j:=0; j< len(srcList); j++ {
//	fmt.Printf("%d: %s\n", j+1, srcList[j].Name)
			if tgtNam == srcList[j].Name {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("tgtNam: %s not found in SourceList!", tgtNam)
		}
	}
	return nil
}
