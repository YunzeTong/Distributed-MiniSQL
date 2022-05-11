package region

import (
	. "Distributed-MiniSQL/common"
	. "Distributed-MiniSQL/minisql"
)

type Bridge struct {
	ftpClient   FtpUtils
	interpreter Interpreter
	api         API
}

func (bridge *Bridge) Construct() {
	bridge.api.Init()
	bridge.ftpClient.Construct() // TODO: we might not need this
}

// I know it looks dirty, just avoid premature optimization
// func (bridge *Bridge) ProcessSQL(sql string) (string, string) {
// 	res, masterRes := bridge.interpreter.Interpret(sql), ""
// 	bridge.api.Store()
// 	bridge.sendTCToFTP()

// 	sqlInfo, resInfo := strings.Split(sql, SEP), strings.Split(res, SEP)

// 	switch resInfo[0] {
// 	case "-->Create":
// 		bridge.sendToFTP(resInfo[2])
// 		masterRes = WrapMessage(PREFIX_REGION, 2, strings.Join([]string{resInfo[2], "add"}, SEP))
// 	case "-->Drop":
// 		bridge.deleteFromFTP(resInfo[2])
// 		masterRes = WrapMessage(PREFIX_REGION, 2, strings.Join([]string{resInfo[2], "delete"}, SEP))
// 	case "-->Insert":
// 		bridge.deleteFromFTP(sqlInfo[2])
// 		bridge.sendToFTP(sqlInfo[2])
// 	case "-->Delete":
// 		bridge.deleteFromFTP(sqlInfo[2])
// 		bridge.sendToFTP(sqlInfo[2])
// 	default:
// 	}
// 	return res, masterRes
// }

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
