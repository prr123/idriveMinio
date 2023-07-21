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
    fmt.Printf("******** object info **********\n")
//  fmt.Printf("Bucket: %s\n", info.Bucket)
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
