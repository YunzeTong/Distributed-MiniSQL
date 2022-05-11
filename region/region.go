package region

import (
	"context"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	. "Distributed-MiniSQL/common"
)

type Region struct {
	etcdClient   *clientv3.Client
	masterClient *rpc.Client
	dbBridge     Bridge

	mockTables []string
}

func (region *Region) Init() {
	region.dbBridge.Construct()
	region.mockTables = make([]string, 0)
}

func (region *Region) Run() {
	// connect to local etcd server
	region.etcdClient, _ = clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://" + HOST_ADDR},
		DialTimeout: 5 * time.Second,
	})
	defer region.etcdClient.Close()
	go region.stayOnline()

	// register rpc
	rpc.Register(region)
	rpc.HandleHTTP()
	l, _ := net.Listen("tcp", REGION_PORT)
	go http.Serve(l, nil)

	// connect to master
	region.masterClient, _ = rpc.DialHTTP("tcp", MASTER_ADDR)
}

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
