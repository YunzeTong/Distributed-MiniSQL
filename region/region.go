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
	"Distributed-MiniSQL/minisql/manager/api"
)

type Region struct {
	masterIP string
	hostIP   string
	backupIP string

	etcdClient   *clientv3.Client
	masterClient *rpc.Client
	backupClient *rpc.Client
	fu           FtpUtils
}

func (region *Region) Init(masterIP, hostIP string) {
	region.masterIP = masterIP
	region.hostIP = hostIP

	region.fu.Construct()

	api.Initial()
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
	client, err := rpc.DialHTTP("tcp", region.masterIP+MASTER_PORT)
	if err != nil {
		log.Printf("rpc.DialHTTP err: %v", region.masterIP+MASTER_PORT)
		return
	}
	region.masterClient = client

	for {
		time.Sleep(10 * time.Second)
	}
}

// https://pkg.go.dev/go.etcd.io/etcd@v3.3.27+incompatible/clientv3#Lease
func (region *Region) stayOnline() {
	for {
		resp, err := region.etcdClient.Grant(context.Background(), 5)
		if err != nil {
			log.Printf("etcd grant error")
			continue
		}

		_, err = region.etcdClient.Put(context.Background(), region.hostIP, "", clientv3.WithLease(resp.ID))
		if err != nil {
			log.Printf("etcd put error")
			continue
		}

		ch, err := region.etcdClient.KeepAlive(context.Background(), resp.ID)
		if err != nil {
			log.Printf("etcd keepalive error")
			continue
		}

		for _ = range ch {
		}
	}
}
