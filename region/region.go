package region

import (
	"context"
	"log"
	"net"

	clientv3 "go.etcd.io/etcd/client/v3"

	. "Distributed-MiniSQL/common"
)

type Region struct {
	masterConn net.Conn
	etcdClient *clientv3.Client
	dbBridge   Bridge
}

func (region *Region) Init() {
	region.dbBridge.Construct()
}

// func (region *Region) Run() {
// 	// connect to local etcd server
// 	region.etcdClient, _ = clientv3.New(clientv3.Config{
// 		Endpoints:   []string{"http://" + HOST_ADDR},
// 		DialTimeout: 5 * time.Second,
// 	})
// 	defer region.etcdClient.Close()
// 	go region.stayOnline()

// 	// connect to master
// 	region.masterConn, _ = net.Dial(NETWORK, MASTER_ADDR)
// 	region.masterConn.Write([]byte(WrapMessage(PREFIX_REGION, 1, strings.Join(region.dbBridge.GetTables(), SEP))))
// 	go region.listenToMaster()

// 	// handle incoming connections from clients
// 	ln, _ := net.Listen(NETWORK, REGION_PORT)
// 	for {
// 		conn, _ := ln.Accept()
// 		go region.handleConn(conn)
// 	}
// }

// https://pkg.go.dev/go.etcd.io/etcd@v3.3.27+incompatible/clientv3#Lease
func (region *Region) stayOnline() {
	ip, name := region.getConfig()

	for {
		resp, err := region.etcdClient.Grant(context.Background(), 5)
		if err != nil {
			continue
		}

		_, err = region.etcdClient.Put(context.Background(), name, ip, clientv3.WithLease(resp.ID))
		if err != nil {
			continue
		}

		ch, err := region.etcdClient.KeepAlive(context.Background(), resp.ID)
		if err != nil {
			continue
		}
		for ka := range ch {
			log.Println(ka)
		}
	}
}

func (region *Region) getConfig() (string, string) {
	// TODO
	ip, name := "", ""
	return ip, name
}

// func (region *Region) listenToMaster() {
// 	reader := bufio.NewReader(region.masterConn)
// 	for {
// 		line, _ := reader.ReadString('\n') // no error
// 		// TODO: line == "" ?
// 		_, opt, info := ParseMessage(line)
// 		switch opt {
// 		case 3:
// 			if info != "" {
// 				info := strings.Split(info, "#")
// 				ip, tables := info[0], strings.Split(info[1], "@")
// 				for _, table := range tables {
// 					region.dbBridge.RestoreTable(table)
// 				}
// 				// TODO: refactor the following within this block?
// 				region.dbBridge.ftpClient.DownloadFile("catalog", ip+"#table_catalog", "")
// 				region.dbBridge.ftpClient.DownloadFile("catalog", ip+"#index_catalog", "")
// 			}
// 			region.dbBridge.api.Init()
// 		case 4:
// 			tables := region.dbBridge.GetTables()
// 			for _, table := range tables {
// 				// TODO: refactor this block: call dbBridge.ProcessSQL instead?
// 				sql := "drop table " + table + ";"
// 				region.dbBridge.interpreter.Interpret(sql)
// 				// TODO: why do need need to call the following per table?
// 				region.dbBridge.api.Store()
// 				region.dbBridge.api.Init()
// 			}
// 		}
// 	}
// }

// func (region *Region) handleConn(conn net.Conn) {
// 	defer conn.Close()

// 	reader := bufio.NewReader(conn)
// 	for {
// 		sql, err := reader.ReadString('\n') // do not use ReadLine
// 		if err != nil {                     // err == io.EOF if connection closed by client
// 			// TODO: connection closed
// 			break
// 		}
// 		// TODO: sql == "" ?
// 		msgClient, msgMaster := region.dbBridge.ProcessSQL(sql)
// 		_, err = conn.Write([]byte(WrapMessage(PREFIX_RESULT, -1, msgClient)))
// 		if err != nil {
// 			// TODO: connection closed
// 		}
// 		if msgMaster != "" {
// 			_, _ = region.masterConn.Write([]byte(WrapMessage(PREFIX_RESULT, -1, msgClient)))
// 		}
// 	}
// }
