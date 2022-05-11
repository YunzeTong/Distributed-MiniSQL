package client

import (
	"fmt"
)

type Client struct {
	ipCache map[string]string
}

type TableOp int

const (
	CREATE = 0
	DROP   = 1
	OTHERS = 2
)

func (client *Client) Init() {

}

func (client *Client) Run() {
	for {
		input := "" // user input
		table, op, err := client.interpret(input)
		if err != nil {
			fmt.Println("input format error")
			continue
		}
		switch op {
		case CREATE:
			// call Master.CreateTable rpc
		case DROP:
			// call Master.DropTable rpc
		case OTHERS:
			ip, ok := client.ipCache[table]
			if !ok {
				ip = client.updateCache(table)
			}
			// call Region.Process rpc with ip var
			hit := false // mock rpc response, false if ip is stale
			if !hit {
				ip = client.updateCache(table)
				// call Region.Process rpc again
			}
		}
	}
}

func (client *Client) interpret(input string) (table string, op TableOp, err error) {
	return "", OTHERS, nil
}

func (client *Client) updateCache(table string) string {
	ip := ""
	// call Master.TableIP rpc
	return ip
}
