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
	etcdClient *clientv3.Client

	regionClients map[string]*rpc.Client

	// operations on the following are in manager.go
	serverTables map[string]*[]string // tables stored on active servers
	tableIP      map[string]string    // table location
}

func (master *Master) Init() {
	master.regionClients = make(map[string]*rpc.Client)
	master.serverTables = make(map[string]*[]string)
	master.tableIP = make(map[string]string)
}

func (master *Master) Run() {
	// connect to local etcd server
	master.etcdClient, _ = clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://" + HOST_ADDR},
		DialTimeout: 5 * time.Second,
	})
	defer master.etcdClient.Close()
	go master.watch()

	rpc.Register(master)
	rpc.HandleHTTP()
	l, _ := net.Listen("tcp", MASTER_PORT)
	go http.Serve(l, nil)
}

func (master *Master) addTable(table, ip string) {
	master.tableIP[table] = ip
	AddUniqueToSlice(master.serverTables[ip], table)
}

func (master *Master) deleteTable(table, ip string) {
	delete(master.tableIP, table)
	DeleteFromSlice(master.serverTables[ip], table)
}

func (master *Master) bestServer(excluded string) string {
	min, res := math.MaxInt, ""
	for ip, pTables := range master.serverTables {
		if ip != excluded && len(*pTables) < min {
			min, res = len(*pTables), ip
		}
	}
	return res
}

func (master *Master) transferServerTables(src, dst string) {
	pTables := master.serverTables[src]
	for _, tab := range *pTables {
		master.tableIP[tab] = dst
		master.addTable(tab, dst)
	}
	delete(master.serverTables, src)
}
