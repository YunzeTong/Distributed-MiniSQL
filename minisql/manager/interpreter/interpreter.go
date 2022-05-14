package interpreter

import (
	index "Distributed-MiniSQL/minisql/manager/commonutil"
	"Distributed-MiniSQL/minisql/manager/indexmanager"
	"Distributed-MiniSQL/minisql/manager/recordmanager"
	"bufio"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"Distributed-MiniSQL/minisql/manager/api"
	"Distributed-MiniSQL/minisql/manager/catalogmanager"
	condition "Distributed-MiniSQL/minisql/manager/commonutil2"
	"Distributed-MiniSQL/minisql/manager/qexception"
)

//全局变量
var nestLock bool = false
var execFile int = 0

const NONEXIST int = -1

var OPERATOR = []string{"<>", "<=", ">=", "=", "<", ">"}

//main函数 line 18
func Main() {
	api.Initial()
	for {
		input, _ := bufio.NewReader(os.Stdin).ReadString('\n')
		//input = "hwggnb!"
		input = strings.TrimRight(input, "\r\n")
		strInterpret(input)
	}
	// if err != nil {

	// }

}

func parseShow(statement string) {
	dataType := strings.Trim(substring(statement, "show", ""), " ")
	if dataType == "tables" {
		catalogmanager.ShowTable()
	} else if dataType == "indexes" {
		catalogmanager.ShowIndex()
	} else {
		panic(qexception.Qexception{0, 323, "Can not find valid key word after 'show'!"})
	}
}

//简化连续正则替换需要多个编译语句的问题
func replaceString(src string, replacement string, reg string) string {
	regexp1, _ := regexp.Compile(reg)
	return regexp1.ReplaceAllString(src, replacement)
}

func parseInsert(statement string) string {
	statement = replaceString(statement, " (", " *\\( *")
	statement = replaceString(statement, ") ", " *\\) *")
	statement = strings.Trim(replaceString(statement, ",", " *, *"), "")
	statement = strings.Trim(replaceString(statement, "", "^insert"), "") //skip insert keyword

	var result strings.Builder
	var startIndex, endIndex int
	if statement == "" {
		result.WriteString("Must add keyword 'into' after insert.")
	}
	endIndex = strings.Index(statement, " ")
	if endIndex == -1 { //no statement after create table xxx
		panic(qexception.Qexception{0, 902, "Not specify the table name"})
	}
	if statement[0:endIndex] != "into" {
		panic(qexception.Qexception{0, 903, "Must add keyword 'into' after insert"})
	}
	startIndex = endIndex + 1
	endIndex = strings.Index(statement[startIndex:], " ") //check table name
	if endIndex == -1 {
		panic(qexception.Qexception{0, 904, "Not specify the insert value"})
	}
	tableName := statement[startIndex:endIndex] //get table name
	startIndex = endIndex + 1
	endIndex = strings.Index(statement[startIndex:], " ") //check values keyword
	if endIndex == -1 {
		panic(qexception.Qexception{0, 905, "Syntax error: Not specify the insert value"})
	}
	if statement[startIndex:endIndex] != "values" {
		panic(qexception.Qexception{0, 906, "Must add keyword 'values' after table " + tableName})
	}
	startIndex = endIndex + 1
	var ok bool
	ok, _ = regexp.MatchString("^\\(.*\\)$", statement[startIndex:])
	if ok {
		panic(qexception.Qexception{0, 907, "Can't not find the insert brackets in table " + tableName})
	}
	var valueParas []string
	valueParas = strings.Split(statement[startIndex+1:], ",")
	var tableRow condition.TableRow
	for i := 0; i < len(valueParas); i++ {
		if i == len(valueParas)-1 {
			valueParas[i] = valueParas[i][0 : len(valueParas[i])-1]
		}
		if valueParas[i] == "" {
			panic(qexception.Qexception{0, 908, "Empty attribute value in insert value"})
		}
		var ok1, ok2 bool
		ok1, _ = regexp.MatchString(valueParas[i], "^\".*\"$")
		ok2, _ = regexp.MatchString(valueParas[i], "^\\'.*\\'$")
		if ok1 || ok2 {
			valueParas[i] = valueParas[i][1 : len(valueParas[i])-1]
		}
		tableRow.AddAttributeValue(valueParas[i])
	}
	if tableRow.GetAttributeSize() != catalogmanager.GetAttributeNum(tableName) {
		panic(qexception.Qexception{1, 909, "Attribute number doesn't match"})
	}
	var attributes []catalogmanager.Attribute
	attributes = catalogmanager.GetTable(tableName).AttributeVector
	for i := 0; i < len(attributes); i++ {
		var attr catalogmanager.Attribute
		attr = attributes[i]
		if attr.IsUnique {
			var cond condition.Condition
			cond = condition.Condition{attr.AttributeName, valueParas[i], "="}
			if catalogmanager.IsIndexKey(tableName, attr.AttributeName) {
				var idx index.Index
				idx = catalogmanager.GetIndex(catalogmanager.GetIndexName(tableName, attr.AttributeName))
				if len(indexmanager.Select(idx, cond)) == 0 {
					continue
				}
			} else {
				var conditions []condition.Condition
				var res []condition.TableRow
				var err bool
				res, err = recordmanager.Select(tableName, conditions)
				if err == true {
					fmt.Println("wrong!!!")
				}
				if len(res) == 0 {
					continue
				}
			}
			panic(qexception.Qexception{1, 910, "Duplicate unique key: " + attr.AttributeName})
		}

	}
	api.InsertRow(tableName, tableRow)
	fmt.Println("")
	result.WriteString("-->Insert successfully")
	return result.String()
}

func substring(str string, start string, end string) string {
	regex := start + "(.*)" + end
	r := regexp.MustCompile(regex)
	matcher := r.FindStringSubmatch(str)
	if matcher == nil {
		return ""
	} else {
		return matcher[1]
	}
}

// line 590 convert 不需要了
//

func contains(str string, reg []string) int {
	for i := 0; i < len(reg); i++ {
		if strings.Contains(str, reg[i]) {
			return i
		}
	}
	return NONEXIST
}

func createCondition(conSet []string) []condition.Condition {
	var c []condition.Condition
	for i := 0; i < len(conSet); i++ {
		index1 := contains(conSet[i], OPERATOR)
		if index1 == NONEXIST {
			panic(qexception.Qexception{0, 999, "Syntax error: Invalid conditions " + conSet[i]})
		}
		attr := strings.Trim(substring(conSet[i], "", OPERATOR[index1]), " ")
		value := strings.Trim(substring(conSet[i], OPERATOR[index1], ""), " ")
		replaceString(value, "", "\\'")
		replaceString(value, "", "\"")
		c = append(c, condition.Condition{attr, value, OPERATOR[index1]})
	}
	return c
}

//没用到
//func checkType(attr string, flag bool) bool {
//	return true
//}

// line 613

// line 620 需要record的接口
func printRow(row condition.TableRow) {
	for i := 0; i < row.GetAttributeSize(); i++ {
		fmt.Println(row.GetAttributeValue(i) + "\t")
	}
	fmt.Println()
}

func getMaxAttrLength(tab []condition.TableRow, index int) int {
	length := 0
	for i := 0; i < len(tab); i++ {
		v := len(tab[i].GetAttributeValue(index))
		if v > length {
			length = v

		}
	}
	return length
}

func printRows(tab []condition.TableRow, tabName string) string {
	var result strings.Builder
	if len(tab) == 0 {
		fmt.Println()
		return "-->Query ok! 0 rows are selected\n"
	}
	attrSize := tab[0].GetAttributeSize()
	cnt := 0
	var v []int
	for j := 0; j < attrSize; j++ {
		length := getMaxAttrLength(tab, j)
		var attrName string
		attrName = catalogmanager.GetAttributeName(tabName, j)
		if len(attrName) > length {
			length = len(attrName)
		}
		v = append(v, length)
		format := "|%-" + strconv.Itoa(length) + "s"
		fmt.Print(format, attrName)
		result.WriteString(fmt.Sprintf(format, attrName))
		//result.append(format).append(attrName)
		cnt = cnt + length + 1
	}
	cnt++

	fmt.Println("|")
	result.WriteString("|\n")

	for i := 0; i < cnt; i++ {
		fmt.Print("-")
		result.WriteString("-")
	}
	fmt.Println()
	result.WriteString("-")
	for i := 0; i < len(tab); i++ {
		var row condition.TableRow
		row = tab[i]
		for j := 0; j < attrSize; j++ {
			format := "|%-" + strconv.Itoa(v[j]) + "s"
			fmt.Print(format, row.GetAttributeValue(j))
			// result.append(format).append(row.getAttributeValue(j))
			result.WriteString(fmt.Sprintf(format, row.GetAttributeValue(j)))
		}
		fmt.Println("|")
		result.WriteString("|\n")
	}
	fmt.Println("-->Query ok! " + strconv.Itoa(len(tab)) + " rows are selected")
	result.WriteString("-->Query ok! ")
	result.WriteString(strconv.Itoa(len(tab)))
	result.WriteString(" rows are selected")
	return result.String()
}

func bufInterpret(reader *bufio.Reader) string {
	restState := ""
	for true {
		var returnValue, statement strings.Builder
		var index1 int
		if strings.Contains(restState, ";") {
			index1 = strings.Index(restState, ";")
			statement.WriteString(restState[0:index1])
			restState = restState[index1+1:]
		} else {
			statement.WriteString(restState)
			statement.WriteString(" ")
			if execFile == 0 {
				fmt.Println("MiniSQL-->")
			}
			for true {
				lineByte, _, _ := reader.ReadLine()
				line := string(lineByte)
				if line == "" {
					break
				} else if strings.Contains(line, ";") {
					index1 = strings.Index(line, ";")
					statement.WriteString(line[0:index1])
					restState = line[index1+1:]
					break
				} else {
					statement.WriteString(line)
					statement.WriteString(" ")
					if execFile == 0 {
						fmt.Println("MiniSQL-->")
					}
				}
			}
		}
		result := strings.Trim(statement.String(), " ")
		result = replaceString(result, " ", "\\s+")
		var tokens []string
		tokens = strings.Split(result, " ")

		if len(tokens) == 1 && tokens[0] == "" {
			panic(qexception.Qexception{0, 200, "No statement specified"})
		}
		switch tokens[0] { //match keyword
		case "create":
			if len(tokens) == 1 {
				panic(qexception.Qexception{0, 201, "Can't find create object"})
			}
			switch tokens[1] {
			case "table":
				parseCreateTable(result)
				break
			case "index":
				parseCreateIndex(result)
				break
			default:
				panic(qexception.Qexception{0, 202, "Can't identify " + tokens[1]})
			}
			break
		case "drop":
			if len(tokens) == 1 {
				panic(qexception.Qexception{0, 203, "Can't find drop object"})
			}
			switch tokens[1] {
			case "table":
				parseDropTable(result)
				break
			case "index":
				parseDropIndex(result)
				break
			default:
				panic(qexception.Qexception{0, 204, "Can't identify " + tokens[1]})
			}
			break
		case "select":
			returnValue.WriteString(parseSelect(result))
			break
		case "insert":
			parseInsert(result)
			break
		case "delete":
			parseDelete(result)
			break
		case "quit":
			parseQuit(result, *reader)
			break
		case "execfile":
			parseSqlFile(result)
			break
		case "show":
			parseShow(result)
			break
		default:
			panic(qexception.Qexception{0, 205, "Can't identify " + tokens[0]})
		}
		//} catch (QException e) {
		//    System.out.println(e.status + " " + QException.ex[e.type] + ": " + e.msg);
		//} catch (Exception e) {
		//    System.out.println("Default error: " + e.getMessage());
		//}

		return returnValue.String()
	}
	//用不到
	return ""
}

// line 39
func strInterpret(sql string) string {
	sql = sql[0 : len(sql)-1]
	resultValue := ""

	regexp1, _ := regexp.Compile("\\s+")
	result := regexp1.ReplaceAllString(strings.Trim(sql, ""), " ")
	var tokens []string
	tokens = strings.Split(result, " ")

	defer func() {
		if err := recover(); err != nil {
			fmt.Println(strconv.Itoa(err.(qexception.Qexception).Status) + " " + qexception.Ex[err.(qexception.Qexception).DataType] + ": " + err.(qexception.Qexception).Msg)
		}

	}()
	//自定义的错误处理机制
	if len(tokens) == 1 && tokens[0] == "" {
		panic(qexception.Qexception{0, 200, "No statement specified"})
	}
	switch tokens[0] {
	case "create":
		if len(tokens) == 1 {
			panic(qexception.Qexception{0, 201, "Can't find create object"})
		}
		switch tokens[1] {
		case "table":
			resultValue += parseCreateTable(result)
			break
		case "index":
			resultValue += parseCreateIndex(result)
			break
		default:
			panic(qexception.Qexception{0, 202, "Can't identify " + tokens[1]})
		}
		break
	case "drop":
		if len(tokens) == 1 {
			panic(qexception.Qexception{0, 203, "Can't find drop object"})
		}
		switch tokens[1] {
		case "table":
			resultValue += parseDropTable(result)
			break
		case "index":
			resultValue += parseDropIndex(result)
			break
		default:
			panic(qexception.Qexception{0, 204, "Can't identify " + tokens[1]})
		}
		break
	case "select":
		resultValue += parseSelect(result)
		break
	case "insert":
		resultValue += parseInsert(result)
		break
	case "delete":
		resultValue += parseDelete(result)
		break
	case "execfile":
		parseSqlFile(result)
		break
	case "show":
		parseShow(result)
		break
	default:
		panic(qexception.Qexception{0, 205, "Can't identify " + tokens[0]})
	}
	return resultValue
}

//另一个私有的非静态interpret函数

func parseDelete(statement string) string {
	//delete from [tabName] where []
	var result strings.Builder
	tabStr := strings.Trim(substring(statement, "from ", " where"), " ")
	conStr := strings.Trim(substring(statement, "where ", ""), " ")
	var conditions []condition.Condition
	if tabStr == "" { //delete from ...
		tabStr = strings.Trim(substring(statement, "from ", ""), " ")
		var conditions []condition.Condition
		num := api.DeleteRow(tabStr, conditions)
		fmt.Println("-->Delete " + strconv.Itoa(num) + " row(s).")
		result.WriteString("-->Delete " + strconv.Itoa(num) + " row(s).")
	} else { //delete from ... where ...
		var conSet []string
		conSet = strings.Split(conStr, " *and *")
		//get condition vector
		conditions = createCondition(conSet)
		num := api.DeleteRow(tabStr, conditions)
		fmt.Println("-->Delete " + strconv.Itoa(num) + " row(s).")
		result.WriteString("-->Delete " + strconv.Itoa(num) + " row(s).")
	}
	return result.String()
}

func parseCreateTable(statement string) string {
	statement = replaceString(statement, " (", " *\\( *")
	statement = replaceString(statement, ") ", " *\\) *")
	statement = strings.Trim(replaceString(statement, ",", " *, *"), "")
	//?????zheng
	statement = strings.Trim(replaceString(statement, "", "^create table "), "")

	var result strings.Builder
	var startIndex, endIndex int
	if statement == "" { //no statement after create table
		panic(qexception.Qexception{0, 401, "Must specify a table name"})
	}
	endIndex = strings.Index(statement, " ")
	if endIndex == -1 { //no statement after create table xxx
		panic(qexception.Qexception{0, 402, "Can't find attribute definition"})
	}
	tableName := statement[0:endIndex] //get table name
	startIndex = endIndex + 1          //start index of '('
	ok := false
	if ok, _ = regexp.MatchString("^\\(.*\\)$", statement[startIndex:len(statement)-1]); !ok { //check brackets
		panic(qexception.Qexception{0, 403, "Can't not find the definition brackets in table " + tableName})
	}
	var length int
	var attrParas, attrsDefine []string
	var attrName, attrType string
	attrLength := ""
	primaryName := ""
	var attrUnique bool
	var attribute catalogmanager.Attribute
	var attrVec []catalogmanager.Attribute

	attrsDefine = strings.Split(statement[startIndex+1:], ",") //get each attribute definition
	for i := 0; i < len(attrsDefine); i++ {                    //for each attribute
		if i == len(attrsDefine)-1 { //last line
			//remove last ')'
			attrsDefine[i] = attrsDefine[i][0 : len(attrsDefine[i])-3]
		}
		attrParas = strings.Split(strings.Trim(attrsDefine[i], ""), " ")
		//split each attribute in parameters: name, type,（length) (unique)

		if attrParas[0] == "" { //empty
			panic(qexception.Qexception{0, 404, "Empty attribute in table " + tableName})
		} else if attrParas[0] == "primary" { //primary key definition
			if len(attrParas) != 3 || attrParas[1] != "key" { //not as primary key xxxx
				panic(qexception.Qexception{0, 405, "Error definition of primary key in table " + tableName})
			}
			ok := true
			if ok, _ = regexp.MatchString("^\\(.*\\)$", attrParas[2]); !ok { //not as primary key (xxxx)
				panic(qexception.Qexception{0, 406, "Error definition of primary key in table " + tableName})
			}
			if primaryName != "" { //already set primary key
				panic(qexception.Qexception{0, 407, "Redefinition of primary key in table " + tableName})
			}

			primaryName = attrParas[2][1 : len(attrParas[2])-1] //set primary key
		} else { //ordinary definition
			if len(attrParas) == 1 { //only attribute name
				panic(qexception.Qexception{0, 408, "Incomplete definition in attribute " + attrParas[0]})
			}
			attrName = attrParas[0]             //get attribute name
			attrType = attrParas[1]             //get attribute type
			for j := 0; j < len(attrVec); j++ { //check whether name redefines
				if attrName == attrVec[j].AttributeName {
					panic(qexception.Qexception{0, 409, "Redefinition in attribute " + attrParas[0]})
				}
			}
			if attrType == "int" || attrType == "float" { //check type
				endIndex = 2 //expected end index
			} else if attrType == "char" {
				if len(attrParas) == 2 { //no char length
					panic(qexception.Qexception{0, 410, "ust specify char length in " + attrParas[0]})
				}
				ok := true
				if ok, _ = regexp.MatchString("^\\(.*\\)$", attrParas[2]); !ok { //not in char (x) form
					panic(qexception.Qexception{0, 411, "Wrong definition of char length in " + attrParas[0]})
				}

				attrLength = attrParas[2][1 : len(attrParas[2])-1] //get length
				var err error
				var length64 int64
				length64, err = strconv.ParseInt(attrLength, 10, 64)
				length = int(length64)
				//check the length
				if err != nil {
					panic(qexception.Qexception{0, 412, "The char length in " + attrParas[0] + " dosen't match a int type or overflow"})
				}
				if length < 1 || length > 255 {
					panic(qexception.Qexception{0, 413, "The char length in " + attrParas[0] + " must be in [1,255] "})
				}
				endIndex = 3 //expected end index
			} else { //unmatched type
				panic(qexception.Qexception{0, 414, "Error attribute type " + attrType + " in " + attrParas[0]})
			}

			if len(attrParas) == endIndex { //check unique constraint
				attrUnique = false
			} else if len(attrParas) == endIndex+1 && attrParas[endIndex] == "unique" { //unique
				attrUnique = true
			} else { //wrong definition
				panic(qexception.Qexception{0, 415, "Error constraint definition in " + attrParas[0]})
			}

			if attrType == "char" { //generate attribute
				length, _ = strconv.Atoi(attrLength)
				attribute = *catalogmanager.NewAttribute(attrName, 1, 4, attrUnique)
			} else if attrType == "int" {
				attribute = *catalogmanager.NewAttribute(attrName, 2, 4, attrUnique)
			} else {
				attribute = *catalogmanager.NewAttribute(attrName, 3, 4, attrUnique)
			}
			attrVec = append(attrVec, attribute)
			//attrVec.add(attribute)
		}
	}
	if primaryName == "" { //check whether set the primary key
		panic(qexception.Qexception{0, 416, "Not specified primary key in table " + tableName})
	}
	var table catalogmanager.Table
	table = *catalogmanager.NewTable(tableName, primaryName, attrVec) // create table
	api.CreateTable(tableName, table)
	fmt.Println("-->Create table " + tableName + " successfully")
	result.WriteString("-->Create table " + tableName + " successfully!")
	return result.String()

}

func parseDropTable(statement string) string {
	var tokens []string
	tokens = strings.Split(statement, " ")
	if len(tokens) == 2 {
		panic(qexception.Qexception{0, 601, "Not specify table name"})
	}
	if len(tokens) != 3 {
		panic(qexception.Qexception{0, 602, "Extra parameters in drop table"})
	}

	tableName := tokens[2] //get table name
	api.DropTable(tableName)
	fmt.Println("-->Drop table " + tableName + " successfully")
	return "-->Drop table " + tableName + " successfully!"
}

func parseCreateIndex(statement string) string {
	statement = replaceString(statement, " ", "\\s+")
	statement = replaceString(statement, " (", " *\\( *")
	statement = replaceString(statement, ") ", " *\\) *")
	statement = strings.Trim(statement, " ")
	var tokens []string
	tokens = strings.Split(statement, " ")
	if len(tokens) == 2 {
		panic(qexception.Qexception{0, 701, "Not specify index name"})
	}
	indexName := tokens[2] //get index name
	if len(tokens) == 3 || tokens[3] != "on" {
		panic(qexception.Qexception{0, 702, "Must add keyword 'on' after index name " + indexName})
	}
	if len(tokens) == 4 {
		panic(qexception.Qexception{0, 703, "Not specify table name"})
	}

	tableName := tokens[4] //get table name
	if len(tokens) == 5 {
		panic(qexception.Qexception{0, 704, "Not specify attribute name in table " + tableName})
	}

	attrName := tokens[5]
	ok := true
	if ok, _ = regexp.MatchString("^\\(.*\\)$", attrName); !ok { //not as (xxx) form
		panic(qexception.Qexception{0, 705, "Error in specifiy attribute name " + attrName})
	}

	attrName = attrName[1 : len(attrName)-1] //extract attribute name
	if len(tokens) != 6 {
		panic(qexception.Qexception{0, 706, "Extra parameters in create index"})
	}
	if !catalogmanager.IsUnique(tableName, attrName) {
		panic(qexception.Qexception{1, 707, "Not a unique attribute"})
	}
	var index1 index.Index
	index1 = *index.NewIndex(indexName, tableName, attrName)
	api.CreateIndex(index1)
	fmt.Println("-->Create index " + indexName + " successfully")
	return "-->Create index " + indexName + " successfully!"
}

func parseDropIndex(statement string) string {
	var tokens []string
	tokens = strings.Split(statement, " ")
	if len(tokens) == 2 {
		panic(qexception.Qexception{0, 801, "Not specify index name"})
	}
	if len(tokens) != 3 {
		panic(qexception.Qexception{0, 802, "Extra parameters in drop index"})
	}
	indexName := tokens[2] //get table name
	api.DropIndex(indexName)
	fmt.Println("-->Drop index " + indexName + " successfully")
	return "-->Drop index " + indexName + " successfully!"
}

// line 582

func parseSelect(statement string) string {
	//select ... from ... where ...
	attrStr := substring(statement, "select ", " from")
	tabStr := substring(statement, "from ", " where")
	conStr := substring(statement, "where ", "")
	var conditions []condition.Condition
	var attrNames []string
	var startTime, endTime int64
	startTime = time.Now().Unix()
	result := ""

	if attrStr == "" {
		panic(qexception.Qexception{0, 250, "Can not find key word 'from' or lack of blank before from!"})
	}
	if strings.Trim(attrStr, " ") == "*" {
		//select all attributes
		if tabStr == "" {
			tabStr = substring(statement, "from ", "")
			var ret []condition.TableRow
			var attriName []string
			var conditions []condition.Condition
			ret = api.Select(tabStr, attriName, conditions)
			endTime = time.Now().Unix()
			result += printRows(ret, tabStr)
		} else { //select * from [] where [];
			var conSet []string
			conSet = strings.Split(conStr, " *and *")
			//get condition vector
			conditions := createCondition(conSet)
			var ret []condition.TableRow //vector
			var attriName []string
			ret = api.Select(tabStr, attriName, conditions)
			endTime = time.Now().Unix()
			result += printRows(ret, tabStr)
		}
	} else {
		attrNames = strings.Split(attrStr, " *, *") //get attributes list
		if tabStr == "" {
			// tabStr = Utils.substring(statement, "from ", "")
			var ret []condition.TableRow
			var conditions []condition.Condition
			ret = api.Select(tabStr, attrNames, conditions)
			endTime = time.Now().Unix()
			result += printRows(ret, tabStr)
		} else {
			var conSet []string
			conSet = strings.Split(conStr, " *and *")
			//get condition vector
			conditions = createCondition(conSet)
			var ret []condition.TableRow
			ret = api.Select(tabStr, attrNames, conditions)
			endTime = time.Now().Unix()
			result += printRows(ret, tabStr)
		}
	}
	var usedTime float64
	floatEndTime := float64(endTime)
	floatStartTime := float64(startTime)
	usedTime = (floatEndTime - floatStartTime) / 1000.0
	fmt.Println("Finished in " + strconv.FormatFloat(usedTime, 'f', 1, 64) + " s")
	result = result + "\nFinished in " + strconv.FormatFloat(usedTime, 'f', 1, 64) + " s"
	return result

}

func parseQuit(statement string, reader bufio.Reader) {
	var tokens []string
	tokens = strings.Split(statement, " ")
	if len(tokens) != 1 {
		panic(qexception.Qexception{0, 1001, "Extra parameters in quit"})
	}
	api.Store()
	fmt.Println("Bye")
	os.Exit(0)
}

func parseSqlFile(statement string) {
	execFile++
	tokens := strings.Split(statement, " ")
	if len(tokens) != 2 {
		panic(qexception.Qexception{0, 1101, "Extra parameters in sql file execution"})
	}
	fileName := tokens[1]
	fi, err := os.Open(fileName)
	if err != nil {
		panic(qexception.Qexception{1, 1103, "Can't find the file"})
	}
	defer fi.Close()
	rd := bufio.NewReader(fi)

	bufInterpret(rd)
	//
	//if ionot {
	//	panic(qexception.Qexception{DataType: 1, Status: 1104, Msg: "IO exception occurs"})
	//}

}
