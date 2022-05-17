package region

import (
	"log"
	"strings"

	. "Distributed-MiniSQL/common"
	api "Distributed-MiniSQL/minisql/manager/api"
	interpreter "Distributed-MiniSQL/minisql/manager/interpreter"
)

// I know it looks dirty, just avoid premature optimization
func (region *Region) ProcessSQL(sql string) string {
	res := interpreter.Interpret(sql)
	api.Store()

	sqlInfo, resInfo := strings.Split(sql, " "), strings.Split(res, " ")

	switch resInfo[0] {
	case "-->Create":
		log.Println("start to add table " + resInfo[2])
		region.sendTableFiles(resInfo[2])
		// AddUniqueToSlice(&region.mockTables, resInfo[2])
		region.sendCatalogFiles()
		log.Println("finish add table " + resInfo[2])
	case "-->Drop":
		log.Println("start to drop table " + resInfo[2])
		region.deleteTableFiles(resInfo[2])
		// DeleteFromSlice(&region.mockTables, resInfo[2])
		region.sendCatalogFiles()
		log.Println("finish drop table " + resInfo[2])
	case "-->Insert":
		log.Printf("%v", sqlInfo[2])
		region.deleteTableFiles(sqlInfo[2])
		region.sendTableFiles(sqlInfo[2])
	case "-->Delete":
		log.Printf("%v", sqlInfo[2])
		region.deleteTableFiles(sqlInfo[2])
		region.sendTableFiles(sqlInfo[2])
	default:
	}
	return res
}

// again, avoid premature optimization
func (region *Region) sendTableFiles(table string) {
	tableFileName := table
	tableIndexFileName := table + "_index.index"
	region.ftpClient.UploadFile(tableFileName, tableFileName)
	region.ftpClient.UploadFile(tableIndexFileName, tableIndexFileName)
}

func (region *Region) deleteTableFiles(table string) {
	region.ftpClient.DeleteFile(table)
	region.ftpClient.DeleteFile(table + "_index.index")
}

func (region *Region) sendCatalogFiles() {
	prefix := region.hostIP + "#"
	tabCatalogName := "table_catalog.txt"
	idxCatalogName := "index_catalog.txt"
	region.ftpClient.UploadFile(tabCatalogName, prefix+tabCatalogName)
	region.ftpClient.UploadFile(idxCatalogName, prefix+idxCatalogName)
}

func (region *Region) GetTables() []string {
	return api.GetTables()
}

func (region *Region) RestoreTable(table string) {
	tableFileName := table
	tableIndexFileName := table + "_index.index"
	DeleteLocalFile(tableFileName)
	DeleteLocalFile(tableIndexFileName)
	region.ftpClient.DownloadFile(tableFileName, DIR+tableFileName, false)
	region.ftpClient.DownloadFile(tableIndexFileName, DIR+tableIndexFileName, false)
}
