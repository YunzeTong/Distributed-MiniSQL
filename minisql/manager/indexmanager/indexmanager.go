package indexmanager

import (
	"bufio"
	"fmt"
	"os"
	"strconv"

	buffermanager "Distributed-MiniSQL/minisql/manager/buffermanager"
	catalogmanager "Distributed-MiniSQL/minisql/manager/catalogmanager"
	index "Distributed-MiniSQL/minisql/manager/commonutil"
	recordmanager "Distributed-MiniSQL/minisql/manager/recordmanager"
)

//TreeMap保存已建立的B+树的信息
var TreeMap = make(map[string]BPTree)

//initialIndex函数,与catalogmanager中的略有不同
func InitIndex() {
	FileName := "./indexCatalog.txt"
	file, err := os.Open(FileName)
	if err != nil {
		fmt.Println("文件打开失败")
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
		CreateIndex(*index.NewIndex2(tempIndexName, tempTableName, tempAttributeName, tempBlockNum, tempRootNum))
		i += 5
	}
}

func CreateIndex(idx index.Index) bool {
	fileName := idx.IndexName + ".index" //只是建立了一个索引文件，实际上的内容只保存在内存中，而没有存到文件里
	BuildIndex(idx)
	file, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		fmt.Println("打开文件错误")
		fmt.Println(err)
		return false
	}
	defer file.Close()

	w := bufio.NewWriter(file)
	fmt.Fprintln(w, idx.IndexName)
	fmt.Fprintln(w, idx.TableName)
	fmt.Fprintln(w, idx.AttributeName)
	fmt.Fprintln(w, strconv.Itoa(idx.BlockNum))
	fmt.Fprint(w, strconv.Itoa(idx.RootNum))
	w.Flush()
	return true
}

func DropIndex(idx index.Index) bool {
	fileName := idx.IndexName + ".index"
	err := os.Remove(fileName) //删除文件
	if err != nil {
		fmt.Println("删除失败")
		return false
	}
	delete(TreeMap, idx.IndexName)
	return true
}

func BuildIndex(idx index.Index) {
	tableName := idx.TableName
	attributeName := idx.AttributeName
	tupleNum := catalogmanager.GetRowNum(tableName)
	tupleLength := GetStoreLength(tableName)
	blockOffset := 0
	processNum := 0
	byteOffset := 4
	IndexNum := catalogmanager.GetAttributeIndex(tableName, attributeName)

	tree := NewBPTree()
	// dataType := catalogmanager.GetType(tableName, IndexNum)
	block := buffermanager.ReadBlockFromDiskQuote(tableName, 0)
	for processNum < tupleNum {
		if byteOffset+tupleLength >= buffermanager.BLOCKSIZE { //寻找下一个block
			blockOffset++
			byteOffset = 0 //重置byteOffset
			block = buffermanager.ReadBlockFromDiskQuote(tableName, blockOffset)
			if block == nil {
				//抛出异常，RuntimeException
			}
		}
		if block.ReadInteger(byteOffset) < 0 {
			value := catalogmanager.NewAddress(tableName, blockOffset, byteOffset)
			row := GetTuple(tableName, *block, byteOffset)
			key := row.GetAttributeValue(IndexNum)
			//
			tree.Insert(key, *value)
			processNum++
		}
		byteOffset += tupleLength
	}
	TreeMap[idx.IndexName] = *tree
}

//需要抛出错误
func Select(idx index.Index, cond recordmanager.Condition) []catalogmanager.Address {
	tree := TreeMap[idx.IndexName]
	indexNum := catalogmanager.GetAttributeIndex(idx.TableName, idx.AttributeName)
	Datatype := catalogmanager.GetType(idx.TableName, indexNum) //类型
	operator := cond.GetOperator()
	var value interface{}
	if Datatype == 1 { //char
		value = cond.GetValue()
	} else if Datatype == 2 { //int
		value, _ = strconv.Atoi(cond.GetValue())
	} else if Datatype == 3 { //float
		value, _ = strconv.ParseFloat(cond.GetValue(), 64)
	}
	if operator == "=" {
		return []catalogmanager.Address{*tree.FindEq(value)}
	} else if operator == "<>" {
		return tree.FindNeq(value)
	} else if operator == ">" {
		return tree.FindGreater(value)
	} else if operator == "<" {
		return tree.FindLess(value)
	} else if operator == ">=" {
		return tree.FindGeq(value)
	} else if operator == "<=" {
		return tree.FindLeq(value)
	} else {
		//抛出异常
	}
	return []catalogmanager.Address{}
}

func Delete(idx index.Index, key string) {
	tree := TreeMap[idx.IndexName]
	indexNum := catalogmanager.GetAttributeIndex(idx.TableName, idx.AttributeName)
	Datatype := catalogmanager.GetType(idx.TableName, indexNum) //类型
	var value interface{}
	if Datatype == 1 { //char
		value = key
	} else if Datatype == 2 { //int
		value, _ = strconv.Atoi(key)
	} else if Datatype == 3 { //float
		value, _ = strconv.ParseFloat(key, 64)
	}
	tree.Delete(value)
}

func Insert(idx index.Index, key string, value catalogmanager.Address) {
	tree := TreeMap[idx.IndexName]
	indexNum := catalogmanager.GetAttributeIndex(idx.TableName, idx.AttributeName)
	Datatype := catalogmanager.GetType(idx.TableName, indexNum) //类型
	var value1 interface{}
	if Datatype == 1 { //char
		value1 = key
	} else if Datatype == 2 { //int
		value1, _ = strconv.Atoi(key)
	} else if Datatype == 3 { //float
		value1, _ = strconv.ParseFloat(key, 64)
	}
	tree.Insert(value1, value)
}

func Update(idx index.Index, key string, value catalogmanager.Address) {
	tree := TreeMap[idx.IndexName]
	indexNum := catalogmanager.GetAttributeIndex(idx.TableName, idx.AttributeName)
	Datatype := catalogmanager.GetType(idx.TableName, indexNum) //类型
	var value1 interface{}
	if Datatype == 1 { //char
		value1 = key
	} else if Datatype == 2 { //int
		value1, _ = strconv.Atoi(key)
	} else if Datatype == 3 { //float
		value1, _ = strconv.ParseFloat(key, 64)
	}
	tree.Insert(value1, value)
}

//一条元组需要多长来存储
func GetStoreLength(tableName string) int {
	rowLen := catalogmanager.GetRowLength(tableName)
	if rowLen > 4 { // a valid byte
		return rowLen + 1
	} else {
		return 5 //4 + 1
	}
}

func GetTuple(tableName string, block buffermanager.Block, offset int) recordmanager.TableRow {
	attributeNum := catalogmanager.GetAttributeNum(tableName)
	result := recordmanager.NewTableRow([]string{})
	var attributeValue string

	offset++ //跳过第一个标志位
	for i := 0; i < attributeNum; i++ {
		length := catalogmanager.GetLength2(tableName, i)
		datatype := catalogmanager.GetType(tableName, i)
		if datatype == 1 { //char
			attributeValue = block.ReadString(offset, length)
			first := int(attributeValue[0]) //存疑
			if first == -1 {
				first = len(attributeValue)
			}
			attributeValue = attributeValue[0:first] //存疑
		} else if datatype == 2 { //int
			attributeValue = strconv.FormatInt(int64(block.ReadInteger(offset)), 10) //写入int
		} else if datatype == 3 { //float
			attributeValue = strconv.FormatFloat(float64(block.ReadFloat(offset)), 'f', 5, 64)
		}
		offset += length
		result.AddAttributeValue(attributeValue)
	}
	return *result
}
