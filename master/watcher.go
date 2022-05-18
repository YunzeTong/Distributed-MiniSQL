package master

import (
	"context"
	"log"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	// . "Distributed-MiniSQL/common"
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
					master.handlePut(ip)
				case mvccpb.DELETE:
					master.handleDelete(ip)
				}
			}

			log.Println("watch chan closed")
		}
	}
}

func (master *Master) handlePut(ip string) {
	// master.backupInfo[ip] = backupIP

	// var err error
	// client, ok := master.regionClients[ip]
	// if !ok {
	// 	client, err = rpc.DialHTTP("tcp", ip+REGION_PORT)
	// 	if err != nil {
	// 		log.Printf("rpc.DialHTTP err: %v", ip+REGION_PORT)
	// 	}
	// 	master.regionClients[ip] = client
	// }

	// _, ok = master.serverTables[backupIP]
	// if !ok {
	// 	temp := make([]string, 0)
	// 	master.serverTables[ip] = &temp
	// 	log.Printf("master.serverTables[%v] set", ip)
	// } else {
	// 	var dummyArgs, dummyReply bool
	// 	// assume that region would not go down soon after it's up
	// 	_, _ = TimeoutRPC(client.Go("Region.DownloadSnapshot", &dummyArgs, &dummyReply, nil), TIMEOUT)
	// }
}

func (master *Master) handleDelete(ip string) {
	// pT, ok := master.serverTables[ip]
	// if !ok {
	// 	continue
	// }
	// tables := *pT
	// if len(tables) == 0 {
	// 	continue
	// }
	// bestServer := master.bestServer(ip)
	// client := master.regionClients[bestServer]
	// args, dummy := DownloadBackupArgs{IP: ip, Tables: tables}, false
	// // assume that other regions would not go down shortly after one did
	// _, _ = TimeoutRPC(client.Go("Region.DownloadBackup", &args, &dummy, nil), TIMEOUT)
	// // assume no error
	// master.transferServerTables(ip, bestServer)
}
