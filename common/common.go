package common

import "strings"

type Identity int

const (
	PREFIX_CLIENT = "<client>"
	PREFIX_MASTER = "<master>"
	PREFIX_REGION = "<region>"
	PREFIX_RESULT = "<result>"

	CLIENT Identity = 0
	MASTER Identity = 1
	REGION Identity = 2
	NULL   Identity = 3

	SEP = " "

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

func ParseMessage(msg string) (Identity, int, string) {
	// TODO
	return MASTER, 0, "" // place holder
}

func WrapMessage(prefix string, opt int, msg string) string {
	var builder strings.Builder

	builder.WriteString(prefix)
	if opt != -1 {
		builder.WriteByte('[')
		builder.WriteByte('0' + byte(opt))
		builder.WriteByte(']')
	}
	builder.WriteString(msg)

	return builder.String()
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
