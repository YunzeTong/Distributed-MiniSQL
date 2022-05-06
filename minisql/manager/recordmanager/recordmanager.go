package recordmanager

import (
	catalogmanager "Distributed-MiniSQL/minisql/manager/catalogManager"
	"fmt"
	"strconv"
	"strings"
)

//TableRow类
type TableRow struct {
	AttributeValue []string
}

func NewTableRow(attributeValue []string) *TableRow {
	return &TableRow{AttributeValue: attributeValue}
}

func (r *TableRow) AddAttributeValue(attributeValue string) {
	r.AttributeValue = append(r.AttributeValue, attributeValue)
}

func (r *TableRow) GetAttributeValue(index int) string {
	return r.AttributeValue[index]
}

func (r *TableRow) GetAttributeSize() int {
	return len(r.AttributeValue)
}

//Condition类
type Condition struct {
	name     string
	value    string
	operator string
}

func NewCondition(name string, operator string, value string) *Condition {
	return &Condition{name: name, operator: operator, value: value}
}

func (c *Condition) satisfy(tableName string, data TableRow) bool {
	index := catalogmanager.GetAttributeIndex(tableName, c.name)
	type1 := catalogmanager.GetType(tableName, index)
	// type1:=GetType(tableName,index)
	if type1 == 1 { //1 char
		cmpObject := data.GetAttributeValue(index)
		cmpValue := c.value
		if c.operator == "=" {
			return strings.Compare(cmpObject, cmpValue) == 0
		} else if c.operator == "<>" {
			return strings.Compare(cmpObject, cmpValue) != 0
		} else if c.operator == ">" {
			return strings.Compare(cmpObject, cmpValue) > 0
		} else if c.operator == "<" {
			return strings.Compare(cmpObject, cmpValue) < 0
		} else if c.operator == ">=" {
			return strings.Compare(cmpObject, cmpValue) >= 0
		} else if c.operator == "<=" {
			return strings.Compare(cmpObject, cmpValue) <= 0
		} else {
			return false
		}
	} else if type1 == 2 { //2 int
		cmpObject, err := strconv.Atoi(data.GetAttributeValue(index))
		cmpValue, err := strconv.Atoi(c.value)
		if err != nil {
			fmt.Print(err)
		}
		switch c.operator {
		case "=":
			return cmpObject == cmpValue
		case "<>":
			return cmpObject != cmpValue
		case ">":
			return cmpObject > cmpValue
		case "<":
			return cmpObject < cmpValue
		case "<=":
			return cmpObject <= cmpValue
		case ">=":
			return cmpObject >= cmpValue
		default:
			return false
		}
	} else if type1 == 3 { //3 float
		cmpObject, err := strconv.ParseFloat(data.GetAttributeValue(index), 64)
		cmpValue, err := strconv.ParseFloat(c.value, 64)
		if err != nil {
			fmt.Print(err)
			return false
		}
		if c.operator == "=" {
			return cmpObject == cmpValue
		} else if c.operator == "<>" {
			return cmpObject != cmpValue
		} else if c.operator == ">" {
			return cmpObject > cmpValue
		} else if c.operator == "<" {
			return cmpObject < cmpValue
		} else if c.operator == ">=" {
			return cmpObject >= cmpValue
		} else if c.operator == "<=" {
			return cmpObject <= cmpValue
		} else {
			return false
		}
	} else {
		return false
	}
}

func (c Condition) GetOperator() string {
	return c.operator
}

func (c Condition) GetValue() string {
	return c.value
}

//Recordmanager类
