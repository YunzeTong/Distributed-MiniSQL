package master

// https://github.com/etcd-io/etcd/tree/main/client/v3

import clientv3 "go.etcd.io/etcd/client/v3"

type Watcher struct {
	client clientv3.Client
}
