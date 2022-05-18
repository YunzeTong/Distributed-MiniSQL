package master

// . "Distributed-MiniSQL/common"

// func (master *Master) CreateTable(args *CreateTableArgs, ip *string) error {
// 	log.Printf("Master.CreateTable called: %v %v", args.Table, args.Sql)
// 	_, ok := master.tableIP[args.Table]
// 	if ok {
// 		log.Printf("table exists: %v", args.Table)
// 		return errors.New("table exists")
// 	}
// 	bestServer := master.bestServer("")
// 	log.Printf("best server is %v", bestServer)
// 	client := master.regionClients[bestServer]
// 	dummy := ""

// 	args.Sql = MockCreateTableSQL(args.Table) // debug
// 	call, err := TimeoutRPC(client.Go("Region.Process", &args.Sql, &dummy, nil), TIMEOUT)
// 	if err != nil {
// 		log.Printf("Region.Process timeout")
// 		return err // timeout
// 	}
// 	if call.Error != nil {
// 		log.Printf("Region.Process error: %v", call.Error)
// 		return call.Error // syntax error
// 	}
// 	master.addTable(args.Table, bestServer)
// 	*ip = bestServer
// 	return nil
// }

// func (master *Master) DropTable(table *string, dummy *bool) error {
// 	log.Println("Master.DropTable called")
// 	ip, ok := master.tableIP[*table]
// 	if !ok {
// 		fmt.Println("no table")
// 		return errors.New("no table")
// 	}
// 	// table must exist on corresponding region
// 	client := master.regionClients[ip]

// 	args, dum := MockDropTableSQL(*table), "" // debug
// 	// args, dum := DropTableSQL(*table), false
// 	call, err := TimeoutRPC(client.Go("Region.Process", &args, &dum, nil), TIMEOUT)
// 	if err != nil {
// 		return err // timeout
// 	}
// 	if call.Error != nil {
// 		return call.Error // drop err
// 	}
// 	master.deleteTable(*table, ip)
// 	return nil
// }

// func (master *Master) TableIP(table *string, ip *string) error {
// 	log.Println("Master.TableIP called")
// 	res, ok := master.tableIP[*table]
// 	if !ok {
// 		return errors.New("no table")
// 	}
// 	*ip = res
// 	return nil
// }
