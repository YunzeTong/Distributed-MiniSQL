package api

import (
	"Distributed-MiniSQL/minisql/manager/buffermanager"
	"Distributed-MiniSQL/minisql/manager/catalogmanager"
	index "Distributed-MiniSQL/minisql/manager/commonutil"
	condition "Distributed-MiniSQL/minisql/manager/commonutil2"
	"Distributed-MiniSQL/minisql/manager/indexmanager"
	"Distributed-MiniSQL/minisql/manager/qexception"
	"Distributed-MiniSQL/minisql/manager/recordmanager"
)

func Initial() {
	//错误处理机制
	//var flag bool = true
	//error 判定？

	buffermanager.BufferInit()
	catalogmanager.InitTable()
	catalogmanager.InitIndex()
	indexmanager.InitIndex()

	//if flag == false {
	//	panic(qexception.Qexception{1, 500, "Failed to initialize API!"})
	//}
}

func Store() {
	catalogmanager.StoreCatalog()
	recordmanager.StoreRecord()
}

func DropIndex(indexName string) bool {
	var index1 index.Index
	index1 = catalogmanager.GetIndex(indexName)
	if indexmanager.DropIndex(index1) && catalogmanager.DropIndex(indexName) {
		return true
	}
	panic(qexception.Qexception{1, 507, "Failed to drop index " + index1.AttributeName + " on table " + index1.TableName})

}

func CreateTable(tabName string, tab catalogmanager.Table) bool {
	if recordmanager.CreateTable(tabName) && catalogmanager.CreateTable(tab) {
		indexName := tabName + "_index" //refactor index name
		var index1 index.Index
		index1 = *index.NewIndex(indexName, tabName, catalogmanager.GetPrimaryKey(tabName))
		indexmanager.CreateIndex(index1)   //create index on Index Manager
		catalogmanager.CreateIndex(index1) //create index on Catalog Manager
		return true
	}
	//	throw new qexception(1, 501, "Table " + tabName + " already exist!")
	//} catch (IOException e) {
	//	throw new qexception(1, 502, "Failed to create an index on table " + tabName)
	//}
	panic(qexception.Qexception{1, 503, "Failed to create table " + tabName})
}

func DropTable(tabName string) bool {
	for i := 0; i < catalogmanager.GetAttributeNum(tabName); i++ {
		attrName := catalogmanager.GetAttributeName(tabName, i)
		indexName := catalogmanager.GetIndexName(tabName, attrName) //find index if exists
		if indexName != "" {
			indexmanager.DropIndex(catalogmanager.GetIndex(indexName)) //drop index at Index Manager
		}
	}
	if catalogmanager.DropTable(tabName) && recordmanager.DropTable(tabName) {
		return true
	} else {
		return false
	}
}

func CreateIndex(index index.Index) bool {
	if indexmanager.CreateIndex(index) && catalogmanager.CreateIndex(index) {
		return true
	}
	panic(qexception.Qexception{1, 506, "Failed to create index " + index.AttributeName + " on table " + index.TableName})
}

func InsertRow(tabName string, row condition.TableRow) bool {
	var recordAddr catalogmanager.Address
	tmp, _ := recordmanager.Insert(tabName, row) //insert and get return address
	recordAddr = *tmp
	attrNum := catalogmanager.GetAttributeNum(tabName) //get the number of attribute
	for i := 0; i < attrNum; i++ {
		attrName := catalogmanager.GetAttributeName(tabName, i)
		indexName := catalogmanager.GetIndexName(tabName, attrName) //find index if exists
		if indexName != "" {                                        //index exists, then need to insert the key to BPTree
			var index1 index.Index
			index1 = catalogmanager.GetIndex(indexName)        //get index
			key := row.GetAttributeValue(i)                    //get value of the key
			indexmanager.Insert(index1, key, recordAddr)       //insert to index manager
			catalogmanager.UpdateIndexTable(indexName, index1) //update index
		}
	}
	catalogmanager.AddRowNum(tabName) //update number of records in catalog        return true;
	return true
	//error
}
func DeleteRow(tabName string, conditions []condition.Condition) int {
	var condition1 condition.Condition
	var res bool
	res, condition1 = FindIndexCondition(tabName, conditions)
	numberIfRecords := 0
	if res == true {
		var indexName string
		//???????????????////getOperator??
		indexName = catalogmanager.GetIndexName(tabName, condition1.Name)
		var idx index.Index
		idx = catalogmanager.GetIndex(indexName)
		var addresses []catalogmanager.Address
		addresses = indexmanager.Select(idx, condition1)
		if addresses != nil {
			var err bool
			numberIfRecords, err = recordmanager.Delete2(addresses, conditions)
			if err == false {

			}
		}
	} else {
		var err bool
		numberIfRecords, err = recordmanager.Delete(tabName, conditions)
		if err == false {

		}
	}
	catalogmanager.DeleteRowNum(tabName, numberIfRecords)
	return numberIfRecords
}

func Select(tabName string, attriName []string, conditions []condition.Condition) []condition.TableRow {
	var resultSet []condition.TableRow
	var condition1 condition.Condition
	var res bool
	res, condition1 = FindIndexCondition(tabName, conditions)
	if res == true {
		indexName := catalogmanager.GetIndexName(tabName, condition1.Name)
		var idx index.Index
		idx = catalogmanager.GetIndex(indexName)
		var addresses []catalogmanager.Address
		addresses = indexmanager.Select(idx, condition1)
		if addresses != nil {
			var err bool
			tmp, err := recordmanager.Select2(addresses, conditions)
			resultSet = *tmp
			if err == false {

			}
		}
		//error
	} else {
		var err bool
		resultSet, err = recordmanager.Select(tabName, conditions)
		//error
		if err == false {

		}
	}
	if len(attriName) != 0 {
		//error
		var err bool
		resultSet, err = recordmanager.Project(tabName, resultSet, attriName)
		if err == false {

		}
		return resultSet
	} else {
		return resultSet
	}
}

//vector转为slice实现

func FindIndexCondition(tabName string, conditions []condition.Condition) (bool, condition.Condition) {
	var condition1 condition.Condition
	var flag bool = false
	for i := 0; i < len(conditions); i++ {
		if catalogmanager.GetIndexName(tabName, conditions[i].Name) != "" {
			condition1 = conditions[i]
			conditions = append(conditions[:i], conditions[i+1:]...)
			flag = true
			break
		}
	}
	return flag, condition1

}
