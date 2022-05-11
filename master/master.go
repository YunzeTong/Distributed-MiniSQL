package master

import (
	"bufio"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	. "Distributed-MiniSQL/common"
)

// map values not addressable, yet everything in Go is passed by value
// thus we need pointers to slices
// check https://go.dev/play/p/rvqLX4XFgRK

type Master struct {
	mutex sync.Mutex

	etcdClient *clientv3.Client

	conns map[string]net.Conn

	// operations on the following are in manager.go
	servers      map[string]bool      // historically connected IPs
	serverTables map[string]*[]string // tables stored on active servers
	tableLoc     map[string]string    // table location
}

func (master *Master) Init() {
	master.conns = make(map[string]net.Conn)
	master.servers = make(map[string]bool)
	master.serverTables = make(map[string]*[]string)
	master.tableLoc = make(map[string]string)
}

func (master *Master) Run() {
	// connect to local etcd server
	master.etcdClient, _ = clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://" + HOST_ADDR},
		DialTimeout: 5 * time.Second,
	})
	defer master.etcdClient.Close()
	go master.watch()

	// handle incoming connections
	ln, _ := net.Listen(NETWORK, MASTER_PORT)
	for {
		conn, _ := ln.Accept()
		go master.handleConn(conn)
	}
}

func (master *Master) handleConn(conn net.Conn) {
	defer conn.Close()
	connRecorded := false

	ip := conn.RemoteAddr().(*net.TCPAddr).IP.String() // https://go.dev/ref/spec#Type_assertions

	reader := bufio.NewReader(conn)
	// loop
	for {
		line, err := reader.ReadString('\n') // do not use ReadLine
		if err != nil {                      // err == io.EOF if connection closed by client
			// TODO: connection closed
			break
		}
		// TODO: line == "" ?
		res := ""
		identity, opt, info := ParseMessage(line)
		switch identity {
		case CLIENT:
			res = master.serveClient(opt, info)
		case REGION:
			if !connRecorded {
				master.conns[ip] = conn
				connRecorded = true
			}
			res = master.serveRegion(opt, info, ip)
		}
		if res != "" {
			if _, err := conn.Write([]byte(PREFIX_MASTER + res)); err != nil {
				// TODO: connection closed
				break
			}
		}
	}
}

func (master *Master) serveClient(opt int, msg string) string {
	res := ""
	switch opt {
	case 1:
		ip := master.tableLocation(msg)
		res = WrapMessage(PREFIX_MASTER, 1, strings.Join([]string{ip, msg}, SEP))
	case 2:
		res = WrapMessage(PREFIX_MASTER, 2, strings.Join([]string{master.bestServer(""), msg}, SEP))
	}
	return res
}

func (master *Master) serveRegion(opt int, msg, ip string) string {
	res := ""
	info := strings.Split(msg, SEP)
	switch opt {
	case 1:
		// TODO: shouldn't this be done with zk/etcd?
		if !master.serverExists(ip) {
			master.addServer(ip, info)
		}
	case 2:
		switch info[1] {
		case "delete":
			master.deleteTable(info[0], ip)
		case "add":
			master.addTable(info[0], ip)
		}
	case 3:
		log.Printf("%v data transferred", ip)
	case 4:
		log.Printf("%v recovered", ip)
	}
	return res
}
