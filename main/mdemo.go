package main

import (
	"context"
	"log"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	HOST = "127.0.0.1:2379"
	KEY  = "foo"
)

func main() {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://" + HOST},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Printf("%v", err)
	}
	defer cli.Close()

	channel := cli.Watch(context.Background(), KEY)

	go ticker(cli)

	for watchResp := range channel {
		for _, event := range watchResp.Events {
			log.Printf("%s %q %q\n", event.Type, event.Kv.Key, event.Kv.Value)
		}
	}
}

func ticker(cli *clientv3.Client) {
	for {
		cli.Put(context.Background(), KEY, time.Now().String())
		time.Sleep(5 * time.Second)
	}
}
