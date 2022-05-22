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
		for watchRes := range watchChan {
			for _, event := range watchRes.Events {
				log.Printf("%s %q %q\n", event.Type, event.Kv.Key, event.Kv.Value)
				ip := string(event.Kv.Key)
				switch event.Type {
				case mvccpb.PUT:
					client, err := rpc.DialHTTP("tcp", ip+REGION_PORT)
					if err != nil {
						log.Printf("dial error: %v", err)
					}
					master.regionClients[ip] = client
					if len(master.serverTables) < master.regionCount {
						master.addRegion(ip)
					} else {
						master.placeBackup(ip)
					}
				case mvccpb.DELETE:
					master.regionClients[ip].Close()
					_, ok := master.serverTables[ip]
					if ok {
						backupIP, ok := master.backupInfo[ip]
						if ok {
							master.transferServerTables(ip, backupIP)
							log.Printf("transferred %v's table to %v", ip, backupIP)
						} else {
							log.Printf("%v has no backup", ip)
							master.removeServerTables(ip)
						}
						delete(master.backupInfo, ip)
					} else {
						backedIP, ok := master.getBackedIP(ip)
						if ok {
							client := master.regionClients[backedIP]
							var dummyArgs, dummyReply bool
							call, err := TimeoutRPC(client.Go("Region.RemoveBackup", &dummyArgs, &dummyReply, nil), TIMEOUT_S)
							if err != nil {
								log.Printf("%v's Region.RemoveBackup timeout", backedIP)
							}
							if call.Error != nil {
								log.Printf("%v's Region.RemoveBackup failed", backedIP)
							} else {
								delete(master.backupInfo, backedIP)
							}
						} else {
							log.Printf("%v backs nobody", ip)
						}
					}
				}
			}
			log.Println("watch chan closed")
		}
	}
}

func (master *Master) addRegion(ip string) {
	temp := make([]string, 0)
	master.serverTables[ip] = &temp
	log.Printf("server add %v", ip)
}

func (master *Master) placeBackup(backupIP string) {
	for ip := range master.serverTables {
		_, ok := master.backupInfo[ip]
		if !ok {
			client := master.regionClients[ip]
			log.Printf("backup ip: %v", backupIP)
			var dummy bool
			call, err := TimeoutRPC(client.Go("Region.AssignBackup", &backupIP, &dummy, nil), TIMEOUT_L)
			if err != nil {
				log.Printf("%v's Region.AssignBackup timeout", ip)
			}
			if call.Error != nil {
				log.Printf("%v's Region.AssignBackup failed", ip)
			} else {
				master.backupInfo[ip] = backupIP
			}
			return
		}
	}
}

func (master *Master) getBackedIP(ip string) (string, bool) {
	for regionIP, backupIP := range master.backupInfo {
		if ip == backupIP {
			return regionIP, true
		}
	}
	return "", false
}
