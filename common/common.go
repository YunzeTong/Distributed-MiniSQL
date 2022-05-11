package common

import (
	"errors"
	"net/rpc"
	"time"
)

type Identity int

const (
	TIMEOUT = 1000

	// networking
	// https://pkg.go.dev/net#Listen
	NETWORK = "tcp"

	MASTER_IP   = "127.0.0.1" // pending
	MASTER_PORT = ":4733"
	MASTER_ADDR = MASTER_IP + MASTER_PORT

	REGION_PORT = ":2016"

	// etcd
	HOST_ADDR = "127.0.0.1:2379"
)

type CreateTableArgs struct {
	Table string
	Sql   string
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
func TimeoutRPC(call *rpc.Call, ms int) (interface{}, error) {
	select {
	case res := <-call.Done:
		return res.Reply, nil
	case <-time.After(time.Duration(ms) * time.Millisecond):
		return nil, errors.New("timeout")
	}
}
