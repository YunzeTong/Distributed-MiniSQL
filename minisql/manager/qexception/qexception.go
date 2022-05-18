package qexception

import (
	"fmt"
	"strconv"
)

var Ex = []string{"Syntax error", "Run time error"}

type Qexception struct {
	DataType int    //exception type: 0 for 'syntax error' and 1 for 'rn time error'
	Status   int    //status code
	Msg      string //exception message
}

func newqexception(dataType int, status int, msg string) *Qexception {
	return &Qexception{
		Status:   status,
		DataType: If(dataType >= 0 && dataType <= len(Ex), dataType, 0),
		Msg:      msg,
	}
}
func If(condition bool, trueVal int, falseVal int) int {
	if condition {
		return trueVal
	}
	return falseVal
}

// @override 重载是在写什么
func getMessage(exception Qexception) string {
	return Ex[exception.DataType] + strconv.Itoa(exception.Status) + ": " + exception.Msg
}

func printMsg(exception Qexception) {
	fmt.Println(Ex[exception.DataType] + strconv.Itoa(exception.Status) + ": " + exception.Msg)
}
