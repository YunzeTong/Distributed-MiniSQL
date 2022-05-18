package master

import (
	"errors"
	"log"

	. "Distributed-MiniSQL/common"
)

func (master *Master) CreateTable(args *TableArgs, ip *string) error {
	log.Printf("Master.CreateTable called: %v %v", args.Table, args.Sql)
	_, ok := master.tableIP[args.Table]
	if ok {
		log.Printf("table exists: %v", args.Table)
		return errors.New("table exists")
	}
	bestServer := master.bestServer("")
	log.Printf("best server is %v", bestServer)
	client := master.regionClients[bestServer]

	dummy := ""
	call, err := TimeoutRPC(client.Go("Region.Process", &args.Sql, &dummy, nil), TIMEOUT)
	if err != nil {
		log.Printf("Region.Process timeout")
		return err // timeout
	}
	if call.Error != nil {
		log.Printf("Region.Process error: %v", call.Error)
		return call.Error // syntax error
	}
	master.addTable(args.Table, bestServer)
	*ip = bestServer
	return nil
}

func (master *Master) DropTable(args *TableArgs, dummy *bool) error {
	log.Println("Master.DropTable called")
	ip, ok := master.tableIP[args.Table]
	log.Println(args.Table)
	log.Println(master.tableIP)
	if !ok {
		log.Printf("no table in memory")
		return errors.New("no table")
	}
	// table must exist on corresponding region
	client := master.regionClients[ip]

	dum := ""
	call, err := TimeoutRPC(client.Go("Region.Process", &args.Sql, &dum, nil), TIMEOUT)
	if err != nil {
		log.Printf("Region.Process timeout")
		return err // timeout
	}
	if call.Error != nil {
		log.Printf("Region.Process process error")
		return call.Error // drop err
	}
	master.deleteTable(args.Table, ip)
	return nil
}

func (master *Master) TableIP(table *string, ip *string) error {
	log.Println("Master.TableIP called")
	res, ok := master.tableIP[*table]
	if !ok {
		return errors.New("no table")
	}
	*ip = res
	return nil
}
