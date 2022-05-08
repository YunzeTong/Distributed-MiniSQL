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
	VAL  = "bar"
)

type Wrapper struct {
	cli *clientv3.Client
}

func (w *Wrapper) Init() {
	w.cli, _ = clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://" + HOST},
		DialTimeout: 5 * time.Second,
	})
}

func (w *Wrapper) Clean() {
	w.cli.Close()
}

func (w *Wrapper) observe() {
	channel := w.cli.Watch(context.Background(), KEY)

	for watchResp := range channel {
		for _, event := range watchResp.Events {
			log.Printf("%s %q %q\n", event.Type, event.Kv.Key, event.Kv.Value)
		}
	}
}

func (w *Wrapper) WatchExample() {
	go w.observe()
	w.ticker()
}

func (w *Wrapper) KeepAliveExample() {
	go w.observe()

	resp, _ := w.cli.Grant(context.Background(), 5)
	_, _ = w.cli.Put(context.Background(), KEY, VAL, clientv3.WithLease(resp.ID))
	ch, _ := w.cli.KeepAlive(context.Background(), resp.ID)

	// consume one
	ka := <-ch
	log.Println("ttl:", ka.TTL)
	time.Sleep(30 * time.Second)

	// consume all
	// for ka := range ch {
	// 	fmt.Println("ttl:", ka.TTL)
	// }
}

func (w *Wrapper) ticker() {
	for {
		w.cli.Put(context.Background(), KEY, time.Now().String())
		time.Sleep(5 * time.Second)
	}
}
func main() {
	var w Wrapper
	w.Init()
	defer w.Clean()

	// w.WatchExample()
	w.KeepAliveExample()
}
