// upload.go
// program that lists all Buckets of the idrive account
// Author: prr, azul software
// Date 29 May 2023
// copyright 2023 prr, azul software
//

package minioLib

import (
	"fmt"
//	"log"
	"time"

    "github.com/minio/minio-go/v7"
)

func FindBuckets(tgtList *[]string, srcList []minio.BucketInfo) (err error){

    for i:=0; i< len(*tgtList); i++ {
        tgtNam := (*tgtList)[i]
        found := false
        for j:=0; j< len(srcList); j++ {
//  fmt.Printf("%d: %s\n", j+1, srcList[j].Name)
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


func PrintObjInfo(info *minio.ObjectInfo) {
    fmt.Printf("********** object info ************\n")
    fmt.Printf("Version ID: %s\n", info.VersionID)
    fmt.Printf("Key: %s\n", info.Key)
    fmt.Printf("Etag: %s\n", info.ETag)
    fmt.Printf("Size: %d\n", info.Size)
    fmt.Printf("Mod:  %s\n", info.LastModified.Format(time.RFC1123))
    fmt.Printf("Exp:  %s\n", info.Expires.Format(time.RFC1123))

    fmt.Printf("MetaData keys[%d]:\n", len(info.Metadata))

    for key, val := range info.Metadata {
        fmt.Printf("  key: %s values[%d]: ", key, len(val))
			for j:=0; j<len(val); j++ {
				fmt.Printf("%s", val[j])
			}
		fmt.Println()
    }

    fmt.Printf("UserMetaData keys: %d\n", len(info.UserMetadata))
    for ukey, uval := range info.UserMetadata {
        fmt.Printf("  key: %s val: %s\n", ukey, uval)
    }
    owner := info.Owner
	fmt.Printf("Owner:\n")
    fmt.Printf("  XMLName: %v\n  DisplayName: %s\n  ID: %s\n", owner.XMLName, owner.DisplayName, owner.ID)
    fmt.Printf("******** end object info **********\n")
}

func PrintBuckets(buckets []minio.BucketInfo) {
    fmt.Println("*********** List Buckets ***********")
    fmt.Printf("Buckets: %d\n", len(buckets))
    for _, bucket := range buckets {
        tstr := bucket.CreationDate.Format(time.RFC1123)
        fmt.Printf("Name: %-15s Creation Date: %s\n", bucket.Name, tstr)
    }
    fmt.Println("********* End List Buckets *********")
}

func PrintUploadInfo(info *minio.UploadInfo) {
    fmt.Printf("*** upload info ***\n")
    fmt.Printf("Bucket: %s\n", info.Bucket)
    fmt.Printf("Etag: %s\n", info.ETag)
    fmt.Printf("Size: %d\n", info.Size)
    fmt.Printf("Mod:  %s\n", info.LastModified.Format(time.RFC1123))
    fmt.Printf("Version ID: %s\n", info.VersionID)
}
