package client

import (
	"Distributed-MiniSQL/common"
	"errors"
	"fmt"
	"net/rpc"
	"strings"
)

type Client struct {
	ipCache      map[string]string
	rpcMaster    *rpc.Client
	rpcRegionMap map[string]*rpc.Client // [ip]rpc
}

type TableOp int

const (
	CREATE = 0
	DROP   = 1
	OTHERS = 2
)

func (client *Client) Init() {
	rpcMas, err := rpc.DialHTTP("tcp", common.MASTER_ADDR)
	if err != nil {
		fmt.Printf("[client]connect error: %v", err)
	}
	client.rpcMaster = rpcMas

}

func (client *Client) Run() {
	for {
		// read a complete sql from keyboard, store it in input
		fmt.Println("新消息>>>请输入你想要执行的SQL语句: ")
		input := ""      // user's complete input
		part_input := "" // part of the input, all of them compose input

		for len(part_input) == 0 || part_input[len(part_input)-1] != ';' {
			fmt.Scanln(&part_input)
			if len(part_input) == 0 {
				continue
			}
			input += part_input
			input += " "
		}

		input = strings.Trim(input, "; ")

		fmt.Println("[风神翼龙test in loop]input: " + input)

		if input == "quit" {
			//TODO 直接退出
			break
		}

		table, op, err := client.preprocessInput(input)
		if err != nil {
			fmt.Println("input format error")
			continue
		}
		switch op {
		case CREATE:
			// call Master.CreateTable rpc
			var args common.CreateTableArgs
			args.Table = table
			args.Sql = input
			ip := ""
			commonCreateCall := client.rpcMaster.Go("Master.CreateTable", args, &ip, nil)

		case DROP:
			// call Master.DropTable rpc
			res := false
			// masterDropCall := client.rpcMaster.Go("Master.DropTable", table, &res, nil)
			// MDreplyCall := <-masterDropCall.Done
		case OTHERS:
			ip, ok := client.ipCache[table]
			if !ok {
				ip = client.updateCache(table)
			}
			// call Region.Process rpc with ip var
			// hit := false
			// regionProcessCall := client.rpcMaster.Go("Region.Process", ip, &hit, nil)
			// RPreplyCall := <- regionProcessCall.Done

			hit := false // mock rpc response, false if ip is stale
			if !hit {
				ip = client.updateCache(table)
				// call Region.Process rpc again
				// regionProcessCall = client.rpcMaster.Go("Region.Process", ip, &hit, nil)
				// RPreplyCall = <- regionProcessCall.Done
			}
		}
	}
}

// create格式默认正确写法: create table student (name varchar, id int);
func (client *Client) preprocessInput(input string) (table string, op TableOp, err error) {
	//初始化三个返回值
	table = ""
	op = OTHERS
	err = nil
	//空格替换
	input = strings.ReplaceAll(input, "\\s+", " ")
	words := strings.Split(input, " ")
	if words[0] == "create" {
		op = CREATE
		if len(words) > 3 { // 因为属性在words[3]所以直接默认 > 3而不是>=3
			table = words[2]
			// if strings.Contains(table, "(") { // 如果被划分成了 student(name varchar, ...)
			// 	table = table[0:strings.Index(table, "(")]
			// }
		}
	} else if words[0] == "drop" {
		op = DROP
		if len(words) == 3 {
			table = words[2]
		}
	} else {
		op = OTHERS
		if words[0] == "select" {
			//select语句的表名放在from后面
			for i := 0; i < len(words); i++ {
				if words[i] == "from" && i != (len(words)-1) {
					table = words[i+1]
					break
				}
			}
		} else if words[0] == "insert" || words[0] == "delete" {
			if len(words) >= 3 {
				table = words[2]
			}
		}
	}

	// 只要table仍为""，说明没拿到表名
	if table == "" {
		err = errors.New("No table name in input")
	}
	return table, op, err
}

func (client *Client) updateCache(table string) string {
	ip := ""
	// call Master.TableIP rpc
	client.rpcMaster.Call("Master.TableIP", table, &ip)
	// tableIPCall := client.rpcMaster.Go("Master.TableIP", table, &ip, nil)
	// replyCall := <-tableIPCall.Done
	return ip
}
