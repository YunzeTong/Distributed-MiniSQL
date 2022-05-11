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
	log.Println("ready to call watch")

	watchChan := master.etcdClient.Watch(context.Background(), "", clientv3.WithPrefix())

	log.Println("watch chan get")

	for watchRes := range watchChan {
		for _, event := range watchRes.Events {
			// TODO: add mutex?
			log.Printf("%s %q %q\n", event.Type, event.Kv.Key, event.Kv.Value)
			ip := string(event.Kv.Key)
			switch event.Type {
			case mvccpb.PUT:
				client, ok := master.regionClients[ip]
				if !ok {
					var err error
					client, err = rpc.DialHTTP("tcp", ip+REGION_PORT)
					if err != nil {
						log.Printf("rpc.DialHTTP err: %v", ip+REGION_PORT)
					}
					master.regionClients[ip] = client
				}

				var dummyArgs, dummyReply bool
				// assume that region would not go down soon after it's up
				_, _ = TimeoutRPC(client.Go("Region.RestoreDatabase", &dummyArgs, &dummyReply, nil), TIMEOUT)

				pStaleTables, ok := master.serverTables[ip]
				if ok {
					for _, table := range *pStaleTables {
						master.deleteTable(table, ip)
					}
				} else {
					temp := make([]string, 0)
					master.serverTables[ip] = &temp
					log.Printf("master.serverTables[%v] set", ip)
				}
			case mvccpb.DELETE:
				pT, ok := master.serverTables[ip]
				if !ok {
					log.Printf("no such kv pair in master.serverTables: %v", ip)
				}
				tables := *pT
				if len(tables) == 0 {
					continue
				}
				bestServer := master.bestServer(ip)
				client := master.regionClients[bestServer]
				args, dummy := DownloadBackupArgs{Ip: ip, Tables: tables}, false
				// assume that other regions would not go down shortly after one did
				_, _ = TimeoutRPC(client.Go("Region.DownloadBackup", &args, &dummy, nil), TIMEOUT)
				// assume no error
				master.transferServerTables(ip, bestServer)
			}
		}
	}
}
