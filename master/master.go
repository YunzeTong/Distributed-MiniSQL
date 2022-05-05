package master

import (
	"bufio"
	"log"
	"net"
	"sync"

	. "Distributed-MiniSQL/common"
)

const (
	NETWORK = "tcp"
	ADDR    = ":8080"
)

// map values not addressable, yet everything in Go is passed by value
// thus we need pointers to slices
// check https://go.dev/play/p/rvqLX4XFgRK

type Master struct {
	mutex sync.Mutex

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

func (master *Master) Serve() {
	ln, _ := net.Listen(NETWORK, ADDR) // https://pkg.go.dev/net#Listen
	for {
		conn, _ := ln.Accept()
		go master.handleConn(conn)
	}
}

func (master *Master) handleConn(conn net.Conn) {
	defer conn.Close()
	connRecorded := false

	ip := conn.RemoteAddr().(*net.TCPAddr).IP.String() // https://go.dev/ref/spec#Type_assertions
	// TODO: do we need this?
	// if ip == "127.0.0.1" {
	// 	ip = getHostAddress()
	// }

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

func (master *Master) serveClient(opt int, info []string) string {
	res := ""
	switch opt {
	case 1:
		ip := master.tableLocation(info[0])
		res = WrapMessage(MASTER, 1, []string{ip, info[0]})
	case 2:
		res = WrapMessage(MASTER, 2, []string{master.bestServer(""), info[0]})
	}
	return res
}

func (master *Master) serveRegion(opt int, info []string, ip string) string {
	res := ""
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

// func getHostAddress() string {
// 	return ""
// }
