package recordmanager

import (
	buffermanager "Distributed-MiniSQL/minisql/manager/buffermanager"
	catalogmanager "Distributed-MiniSQL/minisql/manager/catalogmanager"
	condition "Distributed-MiniSQL/minisql/manager/commonutil2"
	indexmanager "Distributed-MiniSQL/minisql/manager/indexmanager"
	"fmt"
	"os"
	"strconv"
	"strings"
)

//Recordmanager类

func CreateTable(tableName string) bool {
	file, err := os.OpenFile(tableName, os.O_RDWR|os.O_CREATE, 0666)
	defer file.Close()
	if err != nil {
		fmt.Print(err)
		return false
	}

	block := buffermanager.ReadBlockFromDiskQuote(tableName, 0)
	if block == nil {
		return false
	} else {
		block.WriteInteger(0, -1)
		return true
	}
}

func DropTable(tableName string) bool {
	err := os.Remove(tableName)
	if err != nil {
		fmt.Print(err)
		return false
	}
	buffermanager.MakeInvalid(tableName)
	return true
}

//第二个返回的布尔值如果为true表示存在异常
func Select(tableName string, conditions []condition.Condition) ([]condition.TableRow, bool) {
	tupleNum := catalogmanager.GetRowNum(tableName)
	storeLen := GetStoreLength(tableName)
	processNum := 0
	byteOffset := 4
	blockOffset := 0
	var result []condition.TableRow
	block := buffermanager.ReadBlockFromDiskQuote(tableName, 0)
	if block == nil {
		fmt.Print("can't get block from buffer")
		return result, true
	}
	if !CheckCondition(tableName, conditions) {
		fmt.Print("the condition can't match the table")
		return result, true
	}
	for {
		if byteOffset+storeLen >= buffermanager.BLOCKSIZE {
			blockOffset++
			byteOffset = 0
			block = buffermanager.ReadBlockFromDiskQuote(tableName, blockOffset)
			if block == nil {
				fmt.Print("can't get block from buffer")
				return result, true
			}
		}
		if block.ReadInteger(byteOffset) < 0 {
			var i int
			newTableRow := GetTuple(tableName, *block, byteOffset)
			for i = 0; i < len(conditions); i++ {
				if !conditions[i].Satisfy(tableName, *newTableRow) {
					break
				}
			}
			if i == len(conditions) {
				result = append(result, *newTableRow)
			}
			processNum++
		}
		blockOffset += storeLen
		if processNum >= tupleNum {
			break
		}
	}
	return result, false
}

func Insert(tableName string, data condition.TableRow) (*catalogmanager.Address, bool) {
	tupleNum := int(catalogmanager.GetRowNum(tableName))
	headBlock := buffermanager.ReadBlockFromDiskQuote(tableName, 0)
	if headBlock == nil {
		fmt.Print("can't get block from buffer")
		return nil, true
	}
	if !CheckRow(tableName, data) {
		fmt.Print("delete data is illegal")
		return nil, true
	}
	headBlock.IsLocked = true
	freeOffset := headBlock.ReadInteger(0)
	var tupleOffset int
	if freeOffset < 0 {
		tupleOffset = tupleNum
	} else {
		tupleOffset = int(freeOffset)
	}
	blockOffset := GetBlockOffset(tableName, tupleOffset)
	byteOffset := GetByteOffset(tableName, tupleOffset)
	insertBlock := buffermanager.ReadBlockFromDiskQuote(tableName, blockOffset)
	if insertBlock == nil {
		headBlock.IsLocked = false
		fmt.Print("can't get the block will be inserted into")
		return nil, true
	}
	if freeOffset >= 0 {
		freeOffset = insertBlock.ReadInteger(byteOffset + 1)
		headBlock.WriteInteger(0, int(freeOffset))
	}
	headBlock.IsLocked = false
	WriteTuples(tableName, data, *insertBlock, byteOffset)
	return catalogmanager.NewAddress(tableName, blockOffset, byteOffset), false

}
func Delete2(address []catalogmanager.Address, conditions []condition.Condition) (int, bool) {
	if len(address) == 0 {
		return 0, false
	}
	sortAddress(&address)
	tableName := address[0].FileName
	blockOffsetPre := -1
	headBlock := buffermanager.ReadBlockFromDiskQuote(tableName, 0)
	var deleteBlock *buffermanager.Block
	if headBlock == nil {
		return 0, true
	}
	if !CheckCondition(tableName, conditions) {
		return 0, true
	}
	headBlock.IsLocked = true
	deleteNum := 0
	for i := 0; i < len(address); i++ {
		blockOffset := address[i].BlockOffset
		byteOffset := address[i].ByteOffset
		tupleOffset := GetTupleOffset(tableName, blockOffset, byteOffset)
		if i == 0 || blockOffset != blockOffsetPre {
			deleteBlock = buffermanager.ReadBlockFromDiskQuote(tableName, blockOffset)
			if deleteBlock == nil {
				headBlock.IsLocked = false
				return deleteNum, false
			}
		}

		if deleteBlock.ReadInteger(byteOffset) < 0 {
			newRow := GetTuple(tableName, *deleteBlock, byteOffset)
			var j int
			for j := 0; j < len(conditions); j++ {
				if !conditions[j].Satisfy(tableName, *newRow) {
					break
				}
			}
			if j == len(conditions) {
				deleteBlock.WriteInteger(byteOffset, 0)
				deleteBlock.WriteInteger(byteOffset+1, int(headBlock.ReadInteger(0)))
				headBlock.WriteInteger(0, tupleOffset)
				deleteNum++
				for k := 0; k < newRow.GetAttributeSize(); k++ {
					attrName := catalogmanager.GetAttributeName(tableName, k)
					if catalogmanager.IsIndexKey(tableName, attrName) {
						indexName := catalogmanager.GetIndexName(tableName, attrName)
						index := catalogmanager.GetIndex(indexName)
						indexmanager.Delete(index, newRow.GetAttributeValue(k))
					}
				}
			}
		}
		blockOffsetPre = blockOffset

	}
	headBlock.IsLocked = false
	return deleteNum, false

}

func Delete(tableName string, conditions []condition.Condition) (int, bool) {
	tupleNum := catalogmanager.GetRowNum(tableName)
	storeLen := GetStoreLength(tableName)
	processNum := 0
	byteOffset := 4
	blockOffset := 0
	deleteNum := 0
	headBlock := buffermanager.ReadBlockFromDiskQuote(tableName, 0)
	laterBlock := headBlock
	if headBlock == nil {
		fmt.Print("this table can't be got ")
		return 0, true
	}
	if !CheckCondition(tableName, conditions) {
		fmt.Print("the conditions have errors")
		return 0, true
	}
	headBlock.IsLocked = true
	for currentNum := 0; processNum < tupleNum; currentNum++ {
		if byteOffset+storeLen >= buffermanager.BLOCKSIZE {
			blockOffset++
			byteOffset = 0
			laterBlock = buffermanager.ReadBlockFromDiskQuote(tableName, blockOffset)
			if laterBlock == nil {
				headBlock.IsLocked = false
				return deleteNum, true
			}
		}
		if laterBlock.ReadInteger(byteOffset) < 0 {
			var i int
			newRow := GetTuple(tableName, *laterBlock, byteOffset)
			for i = 0; i < len(conditions); i++ {
				if !conditions[i].Satisfy(tableName, *newRow) {
					break
				}
			}
			if i == len(conditions) {
				laterBlock.WriteInteger(byteOffset, 0)
				laterBlock.WriteInteger(byteOffset+1, int(headBlock.ReadInteger(0)))
				headBlock.WriteInteger(0, currentNum)
				deleteNum++
				for j := 0; j < newRow.GetAttributeSize(); j++ {
					attrName := catalogmanager.GetAttributeName(tableName, j)
					if catalogmanager.IsIndexKey(tableName, attrName) {
						indexName := catalogmanager.GetIndexName(tableName, attrName)
						index := catalogmanager.GetIndex(indexName)
						indexmanager.Delete(index, newRow.GetAttributeValue(j))
					}
				}
			}
			processNum++
		}
		byteOffset += storeLen
	}
	headBlock.IsLocked = false
	return deleteNum, false
}

func Swap(i, j *catalogmanager.Address) {
	*i, *j = *j, *i
}

func sortAddress(a *([]catalogmanager.Address)) {
	for i := 0; i < len(*a); i++ {
		for j := i + 1; j < len(*a); j++ {
			if (*a)[i].Compare((*a)[j]) {
				Swap(&(*a)[i], &(*a)[j])
			}
		}
	}
}
func Select2(address []catalogmanager.Address, conditions []condition.Condition) (*([]condition.TableRow), bool) {
	if len(address) == 0 {
		return nil, false
	}
	sortAddress(&address)
	tableName := address[0].FileName
	blockOffsetPre := -1
	result := make([]condition.TableRow, 10)
	if !CheckCondition(tableName, conditions) {
		fmt.Print("the conditions have errors")
		return nil, true
	}
	for i := 0; i < len(address); i++ {
		blockOffset := address[i].BlockOffset
		byteOffset := address[i].ByteOffset
		var block buffermanager.Block
		if i == 0 || blockOffset != blockOffsetPre {
			block := buffermanager.ReadBlockFromDiskQuote(tableName, blockOffset)
			if block == nil {
				if i == 0 {
					fmt.Print("get the block of this table fail")
					return nil, true
				}
			}
		}
		if block.ReadInteger(byteOffset) < 0 {
			var j int
			newRow := GetTuple(tableName, block, byteOffset)
			for j = 0; j < len(conditions); j++ {
				if !conditions[j].Satisfy(tableName, *newRow) {
					break
				}
			}
			if j == len(conditions) {
				result = append(result, *newRow)
			}

		}
		blockOffsetPre = blockOffset
	}
	return &result, false
}

func Project(tableName string, result []condition.TableRow, projectName []string) ([]condition.TableRow, bool) {
	projectResult := make([]condition.TableRow, 10)
	for i := 0; i < len(result); i++ {
		newRow := condition.NewTableRow(make([]string, 10))
		for j := 0; j < len(projectName); j++ {
			index := catalogmanager.GetAttributeIndex(tableName, projectName[j])
			if index == -1 {
				fmt.Print("Can't not find attribute " + projectName[j])
				return projectResult, true
			} else {
				newRow.AddAttributeValue(result[i].GetAttributeValue(index))
			}
		}
		projectResult = append(projectResult, *newRow)

	}
	return projectResult, false
}

func StoreRecord() {
	buffermanager.DestructBufferManager()
}
func GetStoreLength(tableName string) int {
	rowLen := catalogmanager.GetRowLength(tableName)
	if rowLen > 4 {
		return rowLen + 1
	} else {
		return 5
	}
}

func GetBlockOffset(tableName string, tupleOffset int) int {
	storeLen := GetStoreLength(tableName)
	tupleInFirst := (buffermanager.BLOCKSIZE - 4) / storeLen
	tupleInNext := (buffermanager.BLOCKSIZE) / storeLen
	if tupleOffset < tupleInFirst {
		return 0
	} else {
		return (tupleOffset-tupleInFirst)/tupleInNext + 1
	}
}

func GetByteOffset(tableName string, tupleOffset int) int {
	storeLen := GetStoreLength(tableName)
	tupleInFirst := (buffermanager.BLOCKSIZE - 4) / storeLen
	tupleInNext := (buffermanager.BLOCKSIZE) / storeLen
	blockOffset := GetBlockOffset(tableName, tupleOffset)
	if blockOffset == 0 {
		return tupleOffset*storeLen + 4
	} else {
		return (tupleOffset - tupleInFirst - (blockOffset-1)*tupleInNext) * storeLen
	}
}

func GetTupleOffset(tableName string, blockOffset int, byteOffset int) int {
	storeLen := GetStoreLength(tableName)
	tupleInFirst := (buffermanager.BLOCKSIZE - 4) / storeLen
	tupleInNext := (buffermanager.BLOCKSIZE) / storeLen
	if blockOffset == 0 {
		return (byteOffset - 4) / storeLen
	} else {
		return tupleInFirst + tupleInNext*(blockOffset-1) + byteOffset/storeLen
	}
}

func GetTuple(tableName string, block buffermanager.Block, offset int) *condition.TableRow {
	attributeNum := catalogmanager.GetAttributeNum(tableName)
	var attributeValue string
	result := condition.NewTableRow(nil)
	offset = offset + 1
	for i := 0; i < attributeNum; i++ {
		length := catalogmanager.GetLength2(tableName, i)
		type1 := catalogmanager.GetType(tableName, i)
		if type1 == 1 {
			attributeValue := block.ReadString(offset, length)
			first := strings.Index(attributeValue, string([]byte{0x00}))
			if first == -1 {
				first = len(attributeValue)
			}
			attributeValue = attributeValue[:first+1]
		} else if type1 == 2 {
			attributeValue = strconv.Itoa(int(block.ReadInteger(offset)))
		} else if type1 == 3 {
			attributeValue = strconv.FormatFloat(float64(block.ReadFloat(offset)), 'f', 2, 32)
		}
		offset += length
		result.AddAttributeValue(attributeValue)
	}
	return result
}

func WriteTuples(tableName string, data condition.TableRow, block buffermanager.Block, offset int) {
	attributeNum := catalogmanager.GetAttributeNum(tableName)
	block.WriteInteger(offset, -1)
	offset++
	for i := 0; i < attributeNum; i++ {
		length := catalogmanager.GetLength2(tableName, i)
		type1 := catalogmanager.GetType(tableName, i)
		if type1 == 1 {
			reset := make([]byte, length)
			for j := 0; j < length; j++ {
				reset[j] = byte(0)
			}
			block.WriteData(offset, reset)
			block.WriteString(offset, data.GetAttributeValue(i))
		} else if type1 == 2 {
			k, _ := strconv.Atoi(data.AttributeValue[i])
			block.WriteInteger(offset, k)
		} else if type1 == 3 {
			k, _ := strconv.ParseFloat(data.AttributeValue[i], 32)
			block.WriteFloat(offset, float32(k))
		}
		offset += length
	}
}

func CheckRow(tableName string, data condition.TableRow) bool {
	if catalogmanager.GetAttributeNum(tableName) != data.GetAttributeSize() {
		fmt.Printf("Attribute number mismatch")
		return false
	}
	for i := 0; i < data.GetAttributeSize(); i++ {
		type1 := catalogmanager.GetType(tableName, i)
		length := catalogmanager.GetLength2(tableName, i)
		if !CheckType(type1, length, data.AttributeValue[i]) {
			return false
		}
	}
	return true
}

func CheckCondition(tableName string, conditions []condition.Condition) bool {
	for i := 0; i < len(conditions); i++ {
		index := catalogmanager.GetAttributeIndex(tableName, conditions[i].Name)
		if index == -1 {
			fmt.Print("Can't not find attribute " + conditions[i].Name)
		}
		type1 := catalogmanager.GetType(tableName, index)
		length := catalogmanager.GetLength2(tableName, index)
		if !CheckType(type1, length, conditions[i].Value) {
			return false
		}
	}
	return true
}

func CheckType(type1 int, length int, value string) bool {
	switch type1 {
	case 2:
		_, err := strconv.Atoi(value)
		if err != nil {
			fmt.Print(value + " doesn't match int type or overflow")
			return false
		}
	case 1:
		if length < len(value) {
			fmt.Print(value + ": the char number must be less than " + strconv.Itoa(len(value)))
			return false
		}
	case 3:
		_, err := strconv.ParseFloat(value, 64)
		if err != nil {
			fmt.Print(value + " doesn't match float type or overflow")
			return false
		}
	}
	return true
}
