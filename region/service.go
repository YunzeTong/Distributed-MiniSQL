package region

import (
	"errors"
	"log"

	. "Distributed-MiniSQL/common"
)

func (region *Region) Process(input *string, res *string) error {
	log.Printf("%v: Region.Process called: %v", region.hostIP, *input)
	*res = region.dbBridge.ProcessSQL(*input)
	if *res == "invalid syntax" { // pending
		return errors.New("syntax error")
	}
	return nil
}

func (region *Region) RestoreDatabase(dummy *bool, res *bool) error {
	log.Printf("%v: Region.RestoreDatabase called", region.hostIP)
	tables := region.dbBridge.GetTables()
	for _, table := range tables {
		// TODO: refactor this block: call dbBridge.ProcessSQL instead?
		region.dbBridge.interpreter.Interpret(DropTableSQL(table))
		// why do we need to call the following per table?
		region.dbBridge.api.Store()
		region.dbBridge.api.Init()
	}
	return nil
}

func (region *Region) DownloadBackup(args *DownloadBackupArgs, res *bool) error {
	log.Printf("%v: Region.DownloadBackup called", region.hostIP)
	// TODO: refactor?
	for _, table := range args.Tables {
		region.dbBridge.RestoreTable(table)
	}
	prefix := args.IP + "#"
	tabCatalogName := "table_catalog"
	idxCatalogName := "index_catalog"
	region.dbBridge.ftpClient.DownloadFile(prefix+tabCatalogName, tabCatalogName, true)
	region.dbBridge.ftpClient.DownloadFile(prefix+idxCatalogName, idxCatalogName, true)
	region.dbBridge.api.Init()
	return nil
}
