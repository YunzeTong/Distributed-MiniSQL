package common

import (
	"fmt"
	"io/ioutil"
	"net/rpc"
	"os"
	"time"
)

type Identity int

const (
	TIMEOUT = 1000

	// networking
	NETWORK = "tcp"

	MASTER_PORT = ":4733"
	REGION_PORT = ":2016"

	// etcd
	HOST_ADDR = "127.0.0.1:2379"

	// minisql files
	WORKING_DIR = "distributed-mini-sql/"
	DIR         = "sql/"
)

type TableArgs struct {
	Table string
	SQL   string
}

type IndexArgs struct {
	Index string
	Table string
	SQL   string
}

func FindElement(pSlice *[]string, str string) int {
	for i, elem := range *pSlice {
		if elem == str {
			return i
		}
	}
	return -1
}

func AddUniqueToSlice(pSlice *[]string, str string) {
	if FindElement(pSlice, str) == -1 {
		*pSlice = append(*pSlice, str)
	}
}

func DeleteFromSlice(pSlice *[]string, str string) bool {
	index := FindElement(pSlice, str)
	if index == -1 {
		return false
	}
	(*pSlice)[index] = (*pSlice)[len(*pSlice)-1]
	*pSlice = (*pSlice)[:len(*pSlice)-1]
	return true
}

// return self IP
func GetHostIP() string {
	return ""
}

func DeleteLocalFile(fileName string) {
	err := os.Remove(fileName)
	if err != nil {
		fmt.Printf("delete local file failed: %v\n", err)
	}
}

// rpc util
func TimeoutRPC(call *rpc.Call, ms int) (*rpc.Call, error) {
	select {
	case res := <-call.Done:
		return res, nil
	case <-time.After(time.Duration(ms) * time.Millisecond):
		return nil, fmt.Errorf("%v timeout", call.ServiceMethod)
	}
}

// fs util
func CleanDir(localDir string) {
	dir, err := ioutil.ReadDir(localDir)
	if err != nil {
		fmt.Println("Can't obtain files in dir")
	}
	for _, d := range dir {
		os.RemoveAll(localDir + d.Name())
	}
}
