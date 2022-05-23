package master

import (
	"fmt"
	"log"

	. "Distributed-MiniSQL/common"
)

func (master *Master) CreateTable(args *TableArgs, ip *string) error {
	log.Printf("Master.CreateTable called")
	_, ok := master.tableIP[args.Table]
	if ok {
		log.Printf("%v already exists", args.Table)
		return fmt.Errorf("%v already exists", args.Table)
	}
	bestServer := master.bestServer()
	client := master.regionClients[bestServer]

	var dummy string
	call, err := TimeoutRPC(client.Go("Region.Process", &args.SQL, &dummy, nil), TIMEOUT_S)
	if err != nil {
		log.Printf("%v's Region.Process timeout", bestServer)
		return err // timeout
	}
	if call.Error != nil {
		log.Printf("%v's Region.Process error: %v", bestServer, call.Error)
		return call.Error // syntax error
	}
	master.addTable(args.Table, bestServer)
	*ip = bestServer
	return nil
}

func (master *Master) DropTable(args *TableArgs, dummyReply *bool) error {
	log.Printf("Master.DropTable called")
	ip, ok := master.tableIP[args.Table]
	if !ok {
		log.Printf("%v not exist", args.Table)
		return fmt.Errorf("%v not exist", args.Table)
	}
	// table must exist on corresponding region
	client := master.regionClients[ip]

	var dummy string
	call, err := TimeoutRPC(client.Go("Region.Process", &args.SQL, &dummy, nil), TIMEOUT_S)
	if err != nil {
		log.Printf("%v's Region.Process timeout", ip)
		return err // timeout
	}
	if call.Error != nil {
		log.Printf("%v's Region.Process process error", ip)
		return call.Error // drop err
	}
	master.deleteTable(args.Table, ip)
	return nil
}

func (master *Master) ShowTables(dummyArgs *bool, tables *[]string) error {
	*tables = make([]string, 0)
	for _, pTables := range master.serverTables {
		*tables = append(*tables, *pTables...)
	}
	return nil
}

func (master *Master) CreateIndex(args *IndexArgs, ip *string) error {
	log.Printf("Master.CreateIndex called")
	_, ok := master.indexInfo[args.Index]
	if ok {
		log.Printf("%v already exists", args.Index)
		return fmt.Errorf("%v already exists", args.Index)
	}
	*ip = master.tableIP[args.Table]
	client := master.regionClients[*ip]

	var dummy string
	call, err := TimeoutRPC(client.Go("Region.Process", &args.SQL, &dummy, nil), TIMEOUT_S)
	if err != nil {
		log.Printf("%v's Region.Process timeout", *ip)
		return err // timeout
	}
	if call.Error != nil {
		log.Printf("%v's Region.Process error: %v", *ip, call.Error)
		return call.Error // syntax error
	}
	master.indexInfo[args.Index] = args.Table
	return nil
}

func (master *Master) DropIndex(args *IndexArgs, dummyReply *bool) error {
	log.Printf("Master.DropIndex called")
	tbl, ok := master.indexInfo[args.Index]
	if !ok {
		log.Printf("%v not exist", args.Index)
		return fmt.Errorf("%v not exist", args.Index)
	}
	// index must exist on corresponding region
	client := master.regionClients[master.tableIP[tbl]]

	var dummy string
	call, err := TimeoutRPC(client.Go("Region.Process", &args.SQL, &dummy, nil), TIMEOUT_S)
	if err != nil {
		log.Printf("%v's Region.Process timeout", master.tableIP[tbl])
		return err // timeout
	}
	if call.Error != nil {
		log.Printf("%v's Region.Process process error", master.tableIP[tbl])
		return call.Error // drop err
	}
	delete(master.indexInfo, args.Index)
	return nil
}

func (master *Master) ShowIndices(dummyArgs *bool, indices *map[string]string) error {
	log.Printf("Master.ShowIndices called")
	*indices = master.indexInfo
	return nil
}

func (master *Master) TableIP(table *string, ip *string) error {
	log.Printf("Master.TableIP called")
	res, ok := master.tableIP[*table]
	if !ok {
		return fmt.Errorf("%v not exist", *table)
	}
	*ip = res
	return nil
}
