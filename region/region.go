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

const (
	BUFF = 10
)

type Region struct {
	etcdClient   *clientv3.Client
	masterIp     string
	masterClient *rpc.Client
	dbBridge     Bridge

	mockTables []string

	// mock
	ip string
}

func (region *Region) Init(ip, masterIp string) {
	region.dbBridge.Construct(masterIp)
	region.mockTables = make([]string, 0)

	// mock
	region.masterIp = masterIp
	region.ip = ip
}

func (region *Region) Run() {
	// connect to local etcd server
	var err error
	region.etcdClient, err = clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://" + HOST_ADDR},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Println(err)
	} else {
		log.Println("etcd success")
	}
	defer region.etcdClient.Close()
	go region.stayOnline()

	// register rpc
	rpc.Register(region)
	rpc.HandleHTTP()
	l, _ := net.Listen("tcp", REGION_PORT)
	go http.Serve(l, nil)

	log.Println("rpc register")

	// connect to master
	region.masterClient, err = rpc.DialHTTP("tcp", region.masterIp+MASTER_PORT)
	if err != nil {
		log.Printf("rpc.DialHTTP err: %v", region.masterIp+MASTER_PORT)
	}

	for {
		time.Sleep(10 * time.Second)
	}
}

// https://pkg.go.dev/go.etcd.io/etcd@v3.3.27+incompatible/clientv3#Lease
func (region *Region) stayOnline() {
	time.Sleep(time.Second * BUFF)

	for {
		log.Printf("%v stayOnline iter", region.ip)
		resp, err := region.etcdClient.Grant(context.Background(), 5)
		if err != nil {
			log.Println("etcd grant error")
			continue
		} else {
			log.Println("etcd grant finish")
		}

		_, err = region.etcdClient.Put(context.Background(), region.ip, "", clientv3.WithLease(resp.ID))
		if err != nil {
			log.Println("etcd put error")
			continue
		} else {
			log.Println("etcd put finish")
		}

		ch, err := region.etcdClient.KeepAlive(context.Background(), resp.ID)
		if err != nil {
			log.Println("etcd keepalive error")
			continue
		} else {
			log.Println("etcd keepalive finish")
		}
		// for ka := range ch {
		// 	log.Println(ka)
		// }
		for _ = range ch {
		}
	}
}

func (region *Region) getConfig() string {
	// TODO
	// ip, name := "", ""
	// return ip, name
	return region.ip
}
