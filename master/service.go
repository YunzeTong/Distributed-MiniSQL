package master

import (
	. "Distributed-MiniSQL/common"
	"errors"
)

func (master *Master) CreateTable(args *CreateTableArgs, ip *string) error {
	_, ok := master.tableIP[args.Table]
	if ok {
		return errors.New("table exists")
	}
	bestServer := master.bestServer("")
	client := master.regionClients[bestServer]
	dummy := false
	call, err := TimeoutRPC(client.Go("Region.Process", &args, &dummy, nil), TIMEOUT)
	if err != nil {
		return err // timeout
	}
	if call.Error != nil {
		return call.Error // syntax error
	}
	master.addTable(args.Table, bestServer)
	*ip = bestServer
	return nil
}

func (master *Master) DropTable(table *string, dummy *bool) error {
	ip, ok := master.tableIP[*table]
	if !ok {
		return errors.New("no table")
	}
	// table must exist on corresponding region
	client := master.regionClients[ip]
	args, dum := DropTableSQL(*table), false
	_, err := TimeoutRPC(client.Go("Region.Process", &args, &dum, nil), TIMEOUT)
	return err // timeout
}

func (master *Master) TableIP(table *string, ip *string) error {
	res, ok := master.tableIP[*table]
	if !ok {
		return errors.New("no table")
	}
	*ip = res
	return nil
}
