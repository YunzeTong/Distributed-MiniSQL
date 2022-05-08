package master

import (
	. "Distributed-MiniSQL/common"
	"context"
	"log"
	"strings"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type Strategy int

func (master *Master) watch() {
	watchChan := master.etcdClient.Watch(context.Background(), "", clientv3.WithPrefix())

	for watchRes := range watchChan {
		for _, event := range watchRes.Events {
			log.Printf("%s %q %q\n", event.Type, event.Kv.Key, event.Kv.Value)
			ip := string(event.Kv.Value)
			switch event.Type {
			case mvccpb.PUT:
				if master.serverExists(ip) {
					temp := make([]string, 0)
					master.serverTables[ip] = &temp
					// TODO: following code in this if-block might be faulty
					conn := master.conns[ip]
					conn.Write([]byte(WrapMessage(PREFIX_MASTER, 4, "recover")))
				}
			case mvccpb.DELETE:
				if master.serverExists(ip) {
					var builder strings.Builder
					pTables := master.serverTables[ip]

					builder.WriteString(ip)
					builder.WriteByte('#')
					builder.WriteString(strings.Join(*pTables, "@"))

					bestServer := master.bestServer(ip)
					master.transferServerTables(ip, bestServer) // TODO: action might not done, use rpc instead

					conn := master.conns[bestServer]
					conn.Write([]byte(WrapMessage(PREFIX_MASTER, 3, builder.String())))
				}
			}
		}
	}
}
