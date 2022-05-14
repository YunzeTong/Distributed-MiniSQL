package common

import (
	"errors"
	"net/rpc"
	"strings"
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
)

type CreateTableArgs struct {
	Table string
	Sql   string
}

type DownloadBackupArgs struct {
	Ip     string
	Tables []string
}

func AddUniqueToSlice(pSlice *[]string, str string) {
	exists := false
	for _, elem := range *pSlice {
		if elem == str {
			exists = true
			break
		}
	}
	if !exists {
		*pSlice = append(*pSlice, str)
	}
}

func DeleteFromSlice(pSlice *[]string, str string) {
	index := -1
	for i, elem := range *pSlice {
		if str == elem {
			index = i
			break
		}
	}

	(*pSlice)[index] = (*pSlice)[len(*pSlice)-1]
	*pSlice = (*pSlice)[:len(*pSlice)-1]
}

// return self IP
func GetHostIP() string {
	return ""
}

func DeleteLocalFile(fileName string) {
	// TODO
}

// rpc util
func TimeoutRPC(call *rpc.Call, ms int) (*rpc.Call, error) {
	select {
	case res := <-call.Done:
		return res, nil
	case <-time.After(time.Duration(ms) * time.Millisecond):
		return nil, errors.New("timeout")
	}
}

// sql util
func DropTableSQL(table string) string {
	var builder strings.Builder
	builder.WriteString("drop table ")
	builder.WriteString(table)
	builder.WriteByte(';')
	return builder.String()
}

// debug
func MockDropTableSQL(table string) string {
	var builder strings.Builder
	builder.WriteString("-->Drop dummy ")
	builder.WriteString(table)
	return builder.String()
}

func MockCreateTableSQL(table string) string {
	var builder strings.Builder
	builder.WriteString("-->Create dummy ")
	builder.WriteString(table)
	return builder.String()
}
