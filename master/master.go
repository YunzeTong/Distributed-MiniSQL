package master

import (
	"math"
	"net"
	"net/http"
	"net/rpc"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	. "Distributed-MiniSQL/common"
)

// map values not addressable, yet everything in Go is passed by value
// thus we need pointers to slices
// check https://go.dev/play/p/rvqLX4XFgRK

type Master struct {
	regionCount int

	etcdClient    *clientv3.Client
	regionClients map[string]*rpc.Client

	serverTables map[string]*[]string // ip->tables
	tableIP      map[string]string    // table->ip
	indexInfo    map[string]string    // index->table

	backupInfo map[string]string
}

func (master *Master) Init(regionCount int) {
	master.regionCount = regionCount

	master.regionClients = make(map[string]*rpc.Client)
	master.serverTables = make(map[string]*[]string)
	master.tableIP = make(map[string]string)
	master.indexInfo = make(map[string]string)

	master.backupInfo = make(map[string]string)
}

func (master *Master) Run() {
	// connect to local etcd server
	master.etcdClient, _ = clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://" + HOST_ADDR},
		DialTimeout: 1 * time.Second,
	})
	defer master.etcdClient.Close()
	go master.watch()

	rpc.Register(master)
	rpc.HandleHTTP()
	l, _ := net.Listen("tcp", MASTER_PORT)
	go http.Serve(l, nil)

	for {
		time.Sleep(10 * time.Second)
	}
}

func (master *Master) addTable(table, ip string) {
	master.tableIP[table] = ip
	AddUniqueToSlice(master.serverTables[ip], table)
}

func (master *Master) deleteTable(table, ip string) {
	delete(master.tableIP, table)
	DeleteFromSlice(master.serverTables[ip], table)
}

func (master *Master) bestServer() string {
	min, res := math.MaxInt, ""
	for ip, pTables := range master.serverTables {
		if len(*pTables) < min {
			min, res = len(*pTables), ip
		}
	}
	return res
}

func (master *Master) transferServerTables(src, dst string) {
	pTables := master.serverTables[src]
	for _, table := range *pTables {
		master.tableIP[table] = dst
	}
	master.serverTables[dst] = pTables
}
