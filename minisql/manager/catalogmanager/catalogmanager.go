package catalogmanager

import (
	"Distributed-MiniSQL/common"
	index "Distributed-MiniSQL/minisql/manager/commonutil"
	"bufio"
	"fmt"
	"os"
	"strconv"
)

const TableFileName = common.DIR + "tableCatalog.txt"

// const TableFileName2 = "./tableCatalog.txt"
const IndexFileName = common.DIR + "indexCatalog.txt"

var Tables = make(map[string]Table)
var Indexs = make(map[string]index.Index)

// type Catalogmanager struct{

// }
func GetTables() map[string]Table {
	return Tables
}

func GetIndexs() map[string]index.Index {
	return Indexs
}

func InitTable() {
	file, err := os.Open(TableFileName)
	if err != nil {
		fmt.Println("表信息文件不存在")
		return
	}
	defer file.Close()
	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	// for z := 0; z < len(lines); z++{
	// 	fmt.Println(lines[z])
	// }
	for i := 0; i < len(lines); {
		var tempAttributeVector []Attribute
		var tempIndexVector []index.Index
		tempTableName := lines[i]
		tempPrimaryKey := lines[i+1]
		// fmt.Println(lines[i+2])
		tempRowNum, _ := strconv.Atoi(lines[i+2])
		// fmt.Println(err)
		tempIndexNum, _ := strconv.Atoi(lines[i+3])
		i = i + 4

		for j := 0; j < tempIndexNum*2; {
			tempIndexName := lines[i+j]
			tempAttributeName := lines[i+j+1]
			tempIndexVector = append(tempIndexVector, *index.NewIndex(tempIndexName, tempTableName, tempAttributeName))
			j = j + 2
		}
		i = i + 2*tempIndexNum
		tempAttributeNum, _ := strconv.Atoi(lines[i])
		i = i + 1
		for k := 0; k < tempAttributeNum*4; {
			tempAttributeName := lines[i+k]
			tempType, _ := strconv.Atoi(lines[i+k+1])
			tempLength, _ := strconv.Atoi(lines[i+k+2])
			tempIsUnique, _ := strconv.ParseBool(lines[i+k+3])
			tempAttributeVector = append(tempAttributeVector, *NewAttribute(tempAttributeName, tempType, tempLength, tempIsUnique))
			k = k + 4
		}
		i = i + 4*tempAttributeNum
		Tables[tempTableName] = *NewTable2(tempTableName, tempPrimaryKey, tempAttributeVector, tempIndexVector, tempRowNum)
	}
	fmt.Println("表信息加载成功")
}

func InitIndex() {
	file, err := os.Open(IndexFileName)
	if err != nil {
		fmt.Println("索引记录文件不存在")
		return
	}
	defer file.Close()
	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	for i := 0; i < len(lines); {
		tempIndexName := lines[i]
		tempTableName := lines[i+1]
		tempAttributeName := lines[i+2]
		tempBlockNum, _ := strconv.Atoi(lines[i+3])
		tempRootNum, _ := strconv.Atoi(lines[i+4])
		Indexs[tempIndexName] = *index.NewIndex2(tempIndexName, tempTableName, tempAttributeName, tempBlockNum, tempRootNum)
		i += 5
	}
	fmt.Println("索引信息文件加载成功")
}

func StoreTable() {
	file, err := os.OpenFile(TableFileName, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0666)
	if err != nil {
		fmt.Println("打开文件错误")
		fmt.Println(err)
		return
	}
	defer file.Close()
	w := bufio.NewWriter(file)
	// fmt.Println(Tables)
	count := 1
	for _, v := range Tables {
		fmt.Fprintln(w, v.TableName)
		// fmt.Println(v.TableName)
		// if err != nil{
		// 	fmt.Println(err)
		// }
		fmt.Fprintln(w, v.PrimaryKey)
		fmt.Fprintln(w, strconv.Itoa(v.RowNum))
		fmt.Fprintln(w, strconv.Itoa(v.IndexNum))
		for i := 0; i < v.IndexNum; i++ {
			fmt.Fprintln(w, v.IndexVector[i].IndexName)
			fmt.Fprintln(w, v.IndexVector[i].AttributeName)
		}
		fmt.Fprintln(w, strconv.Itoa(v.AttributeNum))
		for i := 0; i < v.AttributeNum; i++ {
			fmt.Fprintln(w, v.AttributeVector[i].AttributeName)
			fmt.Fprintln(w, strconv.Itoa(v.AttributeVector[i].FieldType.NumType))
			fmt.Fprintln(w, strconv.Itoa(v.AttributeVector[i].FieldType.length))
			if count == len(Tables) && i == v.AttributeNum-1 {
				fmt.Fprint(w, strconv.FormatBool(v.AttributeVector[i].IsUnique))
			} else {
				fmt.Fprintln(w, strconv.FormatBool(v.AttributeVector[i].IsUnique))
			}
		}
		count += 1
	}
	w.Flush() //writer是带缓存的，会先写入缓存中，用词要调用Flush方法
}

func StoreIndex() {
	file, err := os.OpenFile(IndexFileName, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0666)
	if err != nil {
		fmt.Println("打开文件错误")
		fmt.Println(err)
		return
	}
	defer file.Close()
	w := bufio.NewWriter(file)
	count := 1
	for _, v := range Indexs {
		fmt.Fprintln(w, v.IndexName)
		fmt.Fprintln(w, v.TableName)
		fmt.Fprintln(w, v.AttributeName)
		fmt.Fprintln(w, strconv.Itoa(v.BlockNum))
		if count == len(Indexs) {
			fmt.Fprint(w, strconv.Itoa(v.RootNum))
		} else {
			fmt.Fprintln(w, strconv.Itoa(v.RootNum))
		}
		count += 1
	}
	w.Flush()
}

// func InitCatalog() {
// 	InitTable()
// 	InitIndex()
// }

func StoreCatalog() {
	StoreTable()
	StoreIndex()
}

func ShowTable() {
	length := 9
	for _, v := range Tables {
		if len(v.TableName) > length {
			length = len(v.TableName)
		}
	}
	fmt.Printf("|%-*s|\n", length, "tableName")
	for _, v := range Tables {
		fmt.Printf("|%-*s|\n", length, v.TableName)
	}
}

func ShowIndex() {
	idx, tab, attr := 9, 9, 9
	for _, v := range Indexs {
		if len(v.IndexName) > idx {
			idx = len(v.IndexName)
		}
		if len(v.TableName) > tab {
			tab = len(v.TableName)
		}
		if len(v.AttributeName) > attr {
			attr = len(v.AttributeName)
		}
	}
	fmt.Printf("|%-*s|%-*s|%-*s|\n", idx, "indexName", tab, "tableName", attr, "attribute")
	for _, v := range Indexs {
		fmt.Printf("|%-*s|%-*s|%-*s|\n", idx, v.IndexName, tab, v.TableName, attr, v.AttributeName)
	}
}

func IsPrimaryKey(tableName string, attributeName string) bool {
	value, ok := Tables[tableName]
	if ok {
		return value.PrimaryKey == attributeName
	} else {
		fmt.Printf("The table %s doesn't exist\n", tableName)
	}
	return false
}

func IsUnique(tableName string, attributeName string) bool {
	value, ok := Tables[tableName]
	if ok {
		for i := 0; i < len(value.AttributeVector); i++ {
			if value.AttributeVector[i].AttributeName == attributeName {
				return value.AttributeVector[i].IsUnique
			}
		}
		fmt.Printf("The attribute %s doesn't exist\n", attributeName)
	} else {
		fmt.Printf("The table %s doesn't exist\n", tableName)
	}
	return false
}

func IsIndexKey(tableName string, attributeName string) bool {
	value, ok := Tables[tableName]
	if ok {
		if !IsAttributeExist(tableName, attributeName) {
			fmt.Printf("The attribute %s doesn't exist\n", attributeName)
			return false
		}
		for i := 0; i < len(value.IndexVector); i++ {
			if value.IndexVector[i].AttributeName == attributeName {
				return true
			}
		}
	} else {
		fmt.Printf("The table %s doesn't exist\n", tableName)
	}
	return false
}

func IsAttributeExist(tableName string, attributeName string) bool {
	value, ok := Tables[tableName]
	if ok {
		for i := 0; i < len(value.AttributeVector); i++ {
			if value.AttributeVector[i].AttributeName == attributeName {
				return true
			}
		}
	}
	return false
}

func IsTableExist(tableName string) bool {
	_, ok := Tables[tableName]
	if ok {
		return true
	} else {
		return false
	}
}

func GetIndexName(tableName string, attribute string) string {
	value, ok := Tables[tableName]
	if ok {
		for i := 0; i < len(value.IndexVector); i++ {
			if value.IndexVector[i].AttributeName == attribute {
				return value.IndexVector[i].IndexName
			}
		}
		//fmt.Printf("The attribute %s doesn't have a index\n", attribute)
	} else {
		//fmt.Printf("The table %s doesn't exist\n", tableName)
	}
	return ""
}

func GetAttributeName(tableName string, i int) string {
	return Tables[tableName].AttributeVector[i].AttributeName
}

//获得属性位置
func GetAttributeIndex(tableName string, attributeName string) int {
	value, ok := Tables[tableName]
	if ok {
		for i := 0; i < len(value.AttributeVector); i++ {
			if value.AttributeVector[i].AttributeName == attributeName {
				return i
			}
		}
		fmt.Printf("The attribute %s doesn't exist\n", attributeName)
	} else {
		fmt.Printf("The table %s doesn't exist\n", tableName)
	}
	return -1
}

func GetAttributeType(tableName string, attributeName string) Datatype {
	value, ok := Tables[tableName]
	if ok {
		for i := 0; i < len(value.AttributeVector); i++ {
			if value.AttributeVector[i].AttributeName == attributeName {
				return value.AttributeVector[i].FieldType
			}
		}
		fmt.Printf("The attribute %s doesn't exist\n", attributeName)
	} else {
		fmt.Printf("The table %s doesn't exist\n", tableName)
	}
	return Datatype{}
}

func GetLength(tableName string, attribute string) int {
	value, ok := Tables[tableName]
	if ok {
		for i := 0; i < len(value.AttributeVector); i++ {
			if value.AttributeVector[i].AttributeName == attribute {
				return value.AttributeVector[i].FieldType.length
			}
		}
		fmt.Printf("The attribute %s doesn't exist\n", attribute)
	} else {
		fmt.Printf("The table %s doesn't exist\n", tableName)
	}
	return -1
}

func GetType(tableName string, i int) int {
	return Tables[tableName].AttributeVector[i].FieldType.NumType
}

//用于recordManager中
func GetLength2(tableName string, i int) int {
	return Tables[tableName].AttributeVector[i].FieldType.length
}

func AddRowNum(tableName string) {
	tmp := Tables[tableName]
	tmp.RowNum++
	Tables[tableName] = tmp
}

func DeleteRowNum(tableName string, num int) {
	tmp := Tables[tableName]
	tmp.RowNum -= num
	Tables[tableName] = tmp
}

func UpdateIndexTable(indexName string, tempIndex index.Index) bool {
	_, ok := Indexs[indexName]
	if !ok { //说明index不存在
		return false
	}
	Indexs[indexName] = tempIndex
	return true
}

func CreateTable(newTable Table) bool {
	_, ok := Tables[newTable.TableName]
	if ok {
		return false //说明表已经存在
	}
	Tables[newTable.TableName] = newTable
	return true
}

func DropTable(tableName string) bool {
	_, ok := Tables[tableName]
	if !ok { //说明Table不存在
		return false
	}
	tmpTable := Tables[tableName]
	for i := 0; i < len(tmpTable.IndexVector); i++ {
		delete(Indexs, tmpTable.IndexVector[i].IndexName)
	}
	delete(Tables, tableName)
	return true
}

func CreateIndex(newIndex index.Index) bool {
	_, ok := Indexs[newIndex.TableName]
	if ok {
		return false //说明index已经存在
	}
	tmpTable := Tables[newIndex.TableName]
	tmpTable.IndexVector = append(tmpTable.IndexVector, newIndex)
	tmpTable.IndexNum = len(tmpTable.IndexVector)
	Tables[newIndex.TableName] = tmpTable
	Indexs[newIndex.IndexName] = newIndex
	return true
}

func DropIndex(indexName string) bool {
	_, ok := Indexs[indexName]
	if !ok { //说明index不存在
		return false
	}
	tmpIndex := Indexs[indexName]
	tmpTable := Tables[tmpIndex.TableName]
	for i := 0; i < len(tmpTable.IndexVector); i++ {
		if tmpTable.IndexVector[i].IndexName == indexName {
			tmpTable.IndexVector = append(tmpTable.IndexVector[:i], tmpTable.IndexVector[i+1:]...)
			break
		}
	}
	tmpTable.IndexNum = len(tmpTable.IndexVector)
	Tables[tmpIndex.TableName] = tmpTable
	delete(Indexs, indexName)
	return true
}

func GetTable(tableName string) Table {
	return Tables[tableName]
}

func GetIndex(indexName string) index.Index {
	return Indexs[indexName]
}

func GetPrimaryKey(tableName string) string {
	return Tables[tableName].PrimaryKey
}

func GetRowLength(tableName string) int {
	return Tables[tableName].RowLength
}

func GetAttributeNum(tableName string) int {
	return Tables[tableName].AttributeNum
}

func GetRowNum(tableName string) int {
	return Tables[tableName].RowNum
}
