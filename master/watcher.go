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
		for watchRes := range watchChan {
			for _, event := range watchRes.Events {
				log.Printf("%s %q %q\n", event.Type, event.Kv.Key, event.Kv.Value)
				ip := string(event.Kv.Key)
				switch event.Type {
				case mvccpb.PUT:
					client, err := rpc.DialHTTP("tcp", ip+REGION_PORT)
					if err == nil {
						master.regionClients[ip] = client
						if len(master.serverTables) < master.regionCount {
							master.addRegion(ip)
						} else {
							err := master.placeBackup(ip)
							if err != nil {
								master.addRegion(ip)
							}
						}
					}
				case mvccpb.DELETE:
					client, ok := master.regionClients[ip]
					if ok {
						client.Close()
						delete(master.regionClients, ip)
					}
					_, ok = master.serverTables[ip]
					if ok {
						backupIP, ok := master.backupInfo[ip]
						if ok {
							master.transferServerTables(ip, backupIP)
							log.Printf("transferred %v's tables to %v", ip, backupIP)
							delete(master.backupInfo, ip)
						} else {
							log.Printf("%v has no backup", ip)
							master.removeServerTables(ip)
						}
					} else {
						backedIP, ok := master.getBackedIP(ip)
						if ok {
							client := master.regionClients[backedIP]
							var dummyArgs, dummyReply bool
							_, err := TimeoutRPC(client.Go("Region.RemoveBackup", &dummyArgs, &dummyReply, nil), TIMEOUT_S)
							if err != nil {
								log.Printf("%v's Region.RemoveBackup timeout", backedIP)
							}
							delete(master.backupInfo, backedIP)
						} else {
							log.Printf("%v backs nobody", ip)
						}
					}
				}
			}
		}
	}
}

func (master *Master) addRegion(ip string) {
	log.Printf("add region: %v", ip)
	temp := make([]string, 0)
	master.serverTables[ip] = &temp
}

func (master *Master) placeBackup(backupIP string) error {
	for ip := range master.serverTables {
		_, ok := master.backupInfo[ip]
		if !ok {
			client := master.regionClients[ip]
			var dummy bool
			call, err := TimeoutRPC(client.Go("Region.AssignBackup", &backupIP, &dummy, nil), TIMEOUT_L)
			if err != nil {
				log.Printf("%v's Region.AssignBackup timeout", ip)
				return fmt.Errorf("%v donw", ip)
			}
			if call.Error != nil {
				log.Printf("%v's Region.AssignBackup failed, meaning new server down", ip)
			} else {
				master.backupInfo[ip] = backupIP
			}
			return nil
		}
	}
	return nil
}

func (master *Master) getBackedIP(ip string) (string, bool) {
	for regionIP, backupIP := range master.backupInfo {
		if ip == backupIP {
			return regionIP, true
		}
	}
	return "", false
}
