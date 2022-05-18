package region

import (
	"errors"
	"log"

	. "Distributed-MiniSQL/common"
	api "Distributed-MiniSQL/minisql/manager/api"
	interpreter "Distributed-MiniSQL/minisql/manager/interpreter"
)

func (region *Region) Process(input *string, res *string) error {
	log.Printf("%v: Region.Process called: %v", region.hostIP, *input)
	*res = region.ProcessSQL(*input)
	if *res == "invalid syntax" { // pending
		return errors.New("syntax error")
	}
	return nil
}

func (region *Region) RestoreDatabase(dummy *bool, res *bool) error {
	log.Printf("%v: Region.RestoreDatabase called", region.hostIP)
	tables := region.GetTables()
	for _, table := range tables {
		// TODO: refactor this block: call ProcessSQL instead?
		interpreter.Interpret(DropTableSQL(table))
		// why do we need to call the following per table?
		api.Store()
		api.Initial()
	}
	return nil
}

func (region *Region) DownloadBackup(args *DownloadBackupArgs, res *bool) error {
	log.Printf("%v: Region.DownloadBackup called", region.hostIP)
	// TODO: refactor?
	for _, table := range args.Tables {
		region.RestoreTable(table)
	}
	prefix := args.IP + "#"
	tabCatalogName := "table_catalog"
	idxCatalogName := "index_catalog"
	region.ftpClient.DownloadFile(prefix+tabCatalogName, DIR+tabCatalogName, true)
	region.ftpClient.DownloadFile(prefix+idxCatalogName, DIR+idxCatalogName, true)
	api.Initial()
	return nil
}
