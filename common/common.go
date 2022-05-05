package common

import "strings"

type Identity int

const (
	PREFIX_CLIENT = "<client>"
	PREFIX_MASTER = "<master>"
	PREFIX_REGION = "<region>"

	CLIENT Identity = 0
	MASTER Identity = 1
	REGION Identity = 2
	NULL   Identity = 3

	SEP = " "
)

func ParseMessage(msg string) (Identity, int, []string) {
	// TODO
	return MASTER, 0, []string{""} // place holder
}

func WrapMessage(identity Identity, opt int, info []string) string {
	var builder strings.Builder

	builder.WriteString(identityPrefix(identity))
	builder.WriteByte('[')
	builder.WriteByte('0' + byte(opt))
	builder.WriteByte(']')
	builder.WriteString(strings.Join(info, SEP))

	return builder.String()
}

func identityPrefix(identity Identity) string {
	var res string
	switch identity {
	case CLIENT:
		res = PREFIX_CLIENT
	case MASTER:
		res = PREFIX_MASTER
	case REGION:
		res = PREFIX_REGION
	}
	return res
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
