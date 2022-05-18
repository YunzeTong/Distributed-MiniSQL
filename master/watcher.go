package master

import (
	"context"
	"fmt"
	"log"
	"net/rpc"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	. "Distributed-MiniSQL/common"
)

func (master *Master) watch() {
	for {
		watchChan := master.etcdClient.Watch(context.Background(), "", clientv3.WithPrefix())
		log.Println("watch chan get")

		for watchRes := range watchChan {
			for _, event := range watchRes.Events {
				log.Printf("%s %q %q\n", event.Type, event.Kv.Key, event.Kv.Value)
				ip := string(event.Kv.Key)
				switch event.Type {
				case mvccpb.PUT:
					log.Printf("len: %v regionCount: %v", len(master.serverTables), master.regionCount)
					if len(master.serverTables) < master.regionCount {
						master.addRegion(ip)
					} else {
						master.placeBackup(ip)
					}
				case mvccpb.DELETE:
					backupIP, ok := master.backupInfo[ip]
					if ok {
						master.transferServerTables(ip, backupIP)
						delete(master.serverTables, ip)
					} else {
						backedIP := master.getBackedIP(ip)
						client := master.regionClients[backedIP]
						// TODO: call regionIP's RemoveBackup
						var dummyArgs *bool
						var dummyReply *bool
						call, err := TimeoutRPC(client.Go("Region.RemoveBackup", &dummyArgs, &dummyReply, nil), TIMEOUT)
						if err != nil {
							fmt.Println("timeout")
						}
						if call.Error != nil {
							fmt.Printf("remove backup ip %s failed\n", backedIP)
						}
					}
				}
			}

			log.Println("watch chan closed")
		}
	}
}

func (master *Master) addRegion(ip string) {
	_, ok := master.regionClients[ip]
	if ok {
		master.regionClients[ip].Close()
	}
	client, err := rpc.DialHTTP("tcp", ip+REGION_PORT)
	if err != nil {
		log.Printf("dial error: %v", err)
	}
	master.regionClients[ip] = client
	temp := make([]string, 0)
	master.serverTables[ip] = &temp
	log.Printf("server add %v", ip)
}

func (master *Master) placeBackup(backupIP string) {
	for ip, _ := range master.serverTables {
		_, ok := master.backupInfo[ip]
		if !ok {
			master.backupInfo[ip] = backupIP
			client := master.regionClients[ip]
			// TODO: call ip's AssignBackup rpc
			var dummyReply *bool
			call, err := TimeoutRPC(client.Go("Region.AssignBackup", &backupIP, &dummyReply, nil), TIMEOUT)
			if err != nil {
				fmt.Println("timeout")
			}
			if call.Error != nil {
				fmt.Printf("IP %s backup built for %s failed\n", backupIP, ip)
			}
			return
		}
	}
}

func (master *Master) getBackedIP(ip string) string {
	var res string
	for regionIP, backupIP := range master.backupInfo {
		if ip == backupIP {
			res = regionIP
		}
	}
	return res
}
