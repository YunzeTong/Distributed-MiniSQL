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
	mockTables  []string
}

func (bridge *Bridge) Construct() {
	bridge.api.Init()
	bridge.ftpClient.Construct() // TODO: we might not need this
	bridge.mockTables = make([]string, 0)
}

// I know it looks dirty, just avoid premature optimization
func (bridge *Bridge) ProcessSQL(sql string) string {
	res := bridge.interpreter.Interpret(sql)
	bridge.api.Store()
	bridge.sendTCToFTP()

	sqlInfo, resInfo := strings.Split(sql, " "), strings.Split(res, " ")

	switch resInfo[0] {
	case "-->Create":
		// bridge.sendToFTP(resInfo[2])
		log.Println("start to add table " + resInfo[2])
		AddUniqueToSlice(&bridge.mockTables, resInfo[2])
		log.Println("finish add table " + resInfo[2])
	case "-->Drop":
		// bridge.deleteFromFTP(resInfo[2])
		log.Println("start to drop table " + resInfo[2])
		DeleteFromSlice(&bridge.mockTables, resInfo[2])
		log.Println("finish drop table " + resInfo[2])
	case "-->Insert":
		bridge.deleteFromFTP(sqlInfo[2])
		bridge.sendToFTP(sqlInfo[2])
	case "-->Delete":
		bridge.deleteFromFTP(sqlInfo[2])
		bridge.sendToFTP(sqlInfo[2])
	default:
	}
	return res
}

// again, avoid premature optimization
func (bridge *Bridge) sendToFTP(info string) {
	bridge.ftpClient.StoreFile(info, "table", "")
	bridge.ftpClient.StoreFile(info+"_index.index", "index", "")
}

func (bridge *Bridge) deleteFromFTP(info string) {
	bridge.ftpClient.DeleteFile(info, "table")
	bridge.ftpClient.DeleteFile(info+"_index.index", "index")
}

func (bridge *Bridge) sendTCToFTP() {
	bridge.ftpClient.StoreFile("table_catalog", "catalog", GetHostIP())
	bridge.ftpClient.StoreFile("index_catalog", "catalog", GetHostIP())
}

func (bridge *Bridge) GetTables() []string {
	// TODO
	return []string{}
}

func (bridge *Bridge) RestoreTable(table string) {
	DeleteLocalFile(table)
	DeleteLocalFile(table + "_index.index")
	bridge.ftpClient.DownloadFile("table", table, "")
	bridge.ftpClient.DownloadFile("index", table+"_index.index", "")
}
