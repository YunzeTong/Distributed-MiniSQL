package region

import (
	"log"
	"strings"

	. "Distributed-MiniSQL/common"
	. "Distributed-MiniSQL/minisql"
)

type Bridge struct {
	ftpClient   FtpUtils
	interpreter Interpreter
	api         API
	hostIP      *string

	mockTables []string
}

func (bridge *Bridge) Construct(masterIP string, hostIP *string) {
	bridge.api.Init()
	bridge.ftpClient.Construct(masterIP) // TODO: we might not need this
	bridge.hostIP = hostIP
	bridge.mockTables = make([]string, 0)
}

// I know it looks dirty, just avoid premature optimization
func (bridge *Bridge) ProcessSQL(sql string) string {
	res := bridge.interpreter.Interpret(sql)
	bridge.api.Store()

	sqlInfo, resInfo := strings.Split(sql, " "), strings.Split(res, " ")

	switch resInfo[0] {
	case "-->Create":
		log.Println("start to add table " + resInfo[2])
		// bridge.sendTableFiles(resInfo[2])
		AddUniqueToSlice(&bridge.mockTables, resInfo[2])
		// bridge.sendCatalogFiles()
		log.Println("finish add table " + resInfo[2])
	case "-->Drop":
		log.Println("start to drop table " + resInfo[2])
		// bridge.deleteTableFiles(resInfo[2])
		DeleteFromSlice(&bridge.mockTables, resInfo[2])
		// bridge.sendCatalogFiles()
		log.Println("finish drop table " + resInfo[2])
	case "-->Insert":
		log.Printf("%v", sqlInfo[2])
		// bridge.deleteTableFiles(sqlInfo[2])
		// bridge.sendTableFiles(sqlInfo[2])
	case "-->Delete":
		log.Printf("%v", sqlInfo[2])
		// bridge.deleteTableFiles(sqlInfo[2])
		// bridge.sendTableFiles(sqlInfo[2])
	default:
	}
	return res
}

// again, avoid premature optimization
func (bridge *Bridge) sendTableFiles(table string) {
	tableFileName := table
	tableIndexFileName := table + "_index.index"
	bridge.ftpClient.UploadFile(tableFileName, tableFileName)
	bridge.ftpClient.UploadFile(tableIndexFileName, tableIndexFileName)
}

func (bridge *Bridge) deleteTableFiles(table string) {
	bridge.ftpClient.DeleteFile(table)
	bridge.ftpClient.DeleteFile(table + "_index.index")
}

func (bridge *Bridge) sendCatalogFiles() {
	prefix := *bridge.hostIP + "#"
	tabCatalogName := "table_catalog"
	idxCatalogName := "index_catalog"
	bridge.ftpClient.UploadFile(tabCatalogName, prefix+tabCatalogName)
	bridge.ftpClient.UploadFile(idxCatalogName, prefix+idxCatalogName)
}

func (bridge *Bridge) GetTables() []string {
	return bridge.api.GetTables()
}

func (bridge *Bridge) RestoreTable(table string) {
	tableFileName := table
	tableIndexFileName := table + "_index.index"
	DeleteLocalFile(tableFileName)
	DeleteLocalFile(tableIndexFileName)
	bridge.ftpClient.DownloadFile(tableFileName, tableFileName, false)
	bridge.ftpClient.DownloadFile(tableIndexFileName, tableIndexFileName, false)
}
