package master

import (
	"context"
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
}

func (master *Master) placeBackup(backupIP string) {
	for ip, _ := range master.serverTables {
		_, ok := master.backupInfo[ip]
		if !ok {
			master.backupInfo[ip] = backupIP
			client := master.regionClients[ip]
			// TODO: call ip's AssignBackup rpc
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
