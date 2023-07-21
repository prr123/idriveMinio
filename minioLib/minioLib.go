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
