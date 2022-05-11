package master

import "Distributed-MiniSQL/common"

func (master *Master) CreateTable(args common.CreateTableArgs, ip *string) error {
	return nil
}

func (master *Master) DropTable(table string, res *bool) error {
	return nil
}

func (master *Master) TableIP(table string, ip *string) error {
	return nil
}
