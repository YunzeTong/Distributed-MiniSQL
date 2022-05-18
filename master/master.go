package master

import (
	"log"
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

	serverTables map[string]*[]string // tables stored on active servers
	tableIP      map[string]string    // table location
	backupInfo   map[string]string
}

func (master *Master) Init(regionCount int) {
	master.regionCount = regionCount

	master.regionClients = make(map[string]*rpc.Client)
	master.serverTables = make(map[string]*[]string)
	master.tableIP = make(map[string]string)
	master.backupInfo = make(map[string]string)
}

func (master *Master) Run() {
	// connect to local etcd server
	var err error
	master.etcdClient, err = clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://" + HOST_ADDR},
		DialTimeout: 1 * time.Second,
	})
	if err != nil {
		log.Println(err)
	} else {
		log.Println("etcd success")
	}
	defer master.etcdClient.Close()
	go master.watch()

	rpc.Register(master)
	rpc.HandleHTTP()
	l, _ := net.Listen("tcp", MASTER_PORT)
	go http.Serve(l, nil)

	log.Println("rpc register")

	for {
		time.Sleep(10 * time.Second)
	}
}

func (master *Master) addTable(table, ip string) {
	master.tableIP[table] = ip
	AddUniqueToSlice(master.serverTables[ip], table)
	log.Println(master.serverTables[ip])
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
	for _, table := range *pTables {
		master.tableIP[table] = dst
	}
	master.serverTables[dst] = pTables
}
