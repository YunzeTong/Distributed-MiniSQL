package client

import (
	// "Distributed-MiniSQL/common"
	. "Distributed-MiniSQL/common"
	"bufio"
	"errors"
	"fmt"
	"net/rpc"
	"os"
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
	SHOW   = 3
)

func (client *Client) Init(masterIP string) {
	masterAddr := masterIP + MASTER_PORT
	rpcMas, err := rpc.DialHTTP("tcp", masterAddr)
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
			part_input, _ = bufio.NewReader(os.Stdin).ReadString('\n')
			part_input = strings.TrimRight(part_input, "\r\n")
			// fmt.Println("[part test]" + part_input)
			if len(part_input) == 0 {
				continue
			}
			input += part_input
			input += " "
		}

		input = strings.Trim(input, " ")

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
			args := TableArgs{Table: table, Sql: input}
			ip := ""
			call, err := TimeoutRPC(client.rpcMaster.Go("Master.CreateTable", &args, &ip, nil), TIMEOUT)
			if err != nil {
				fmt.Println("timeout")
			}
			if call.Error != nil {
				fmt.Println("[from master to client]create table failed")
			} else {
				fmt.Println("create table succeed, table in ip: " + ip)
			}
		case DROP:
			// call Master.DropTable rpc
			args := TableArgs{Table: table, Sql: input}
			dummy := false
			call, err := TimeoutRPC(client.rpcMaster.Go("Master.DropTable", &args, &dummy, nil), TIMEOUT)
			if err != nil {
				fmt.Println("timeout")
			}
			if call.Error != nil {
				fmt.Println("[error]drop table failed")
			}
		case OTHERS:
			// by default: only ip in ipCache, rpcregion will exist in rpcmap
			ip, ok := client.ipCache[table]
			if !ok {
				ip = client.updateCache(table)
				if ip == "" {
					fmt.Println("can't find the corresponding ip in cache")
					break
				}
			} else {
				fmt.Println("find corresponding ip in table-ip map: " + ip)
			}
			// call Region.Process rpc with ip var
			result := ""
			// obtain regionRPC
			rpcRegion, ok := client.rpcRegionMap[ip]
			if !ok {
				fmt.Printf("region not in RPCcache, add it to map")
				rpcRegion, err = rpc.DialHTTP("tcp", ip)
				if err != nil {
					fmt.Println("fail to connect to region: " + ip)
					fmt.Println("IP is new but can't connect")
					delete(client.ipCache, table)
					break
				} else {
					// client.ipCache[table] = ip
					_, ok := client.ipCache[table]
					if !ok {
						fmt.Println("[正常情况下不会出现此打印]no ip-table cache in map")
					}
					client.rpcRegionMap[ip] = rpcRegion
				}
			} else {
				fmt.Println("[最终可删除]first phase: find corresponding rpc in rpc map")
			}

			call, err := TimeoutRPC(rpcRegion.Go("Region.Process", &input, &result, nil), TIMEOUT)
			if err != nil {
				fmt.Println("[region process]timeout")
			}
			if call.Error != nil {
				//TODO: 这里的error有可能是sql错误导致或者ip旧了rpcRegion拿不到导致，先放一放
				fmt.Println("can't obtain result, maybe old ip or sql error")

				// ip, rpcRegion is old，select for twice
				// first delete old cache
				delete(client.ipCache, table)
				delete(client.rpcRegionMap, ip)
				new_ip := client.updateCache(table) // obtain newest ip
				if new_ip == "" {
					fmt.Println("can't find the ip which table in")
					break
				}
				// obtain newest rpcRegion and update map
				new_rpcRegion, err := rpc.DialHTTP("tcp", new_ip)
				if err != nil {
					fmt.Printf("[client]fail to connect to region: " + ip)
					delete(client.ipCache, table)
					break
				}
				// call Region.Process rpc again
				call, err := TimeoutRPC(new_rpcRegion.Go("Region.Process", &input, &result, nil), TIMEOUT)
				if err != nil {
					fmt.Println("[no cache and region process]timeout")
					break
				}
				if call.Error != nil {
					fmt.Println("can't obatin result, maybe input is error")
					break
				}
				fmt.Println("result: " + result)
				client.ipCache[table] = new_ip
				client.rpcRegionMap[new_ip] = new_rpcRegion
				fmt.Println("[最终不一定非得删除]update ip: " + ip + " and add it to iptablemap")
			}
			fmt.Println("result: " + result)
		case SHOW:
			showChoice := table
			fmt.Println("show " + showChoice)
			// TODO: 调用show
			//result =
			//fmt.Println(result)
		}

	}
}

// create格式默认正确写法: create table student (name varchar, id int);
func (client *Client) preprocessInput(input string) (table string, op TableOp, err error) {
	//初始化三个返回值
	input = strings.Trim(input, ";")
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
			fmt.Println("[最终可删]drop table" + words[2])
			table = words[2]
		}
	} else {
		op = OTHERS
		if words[0] == "select" {
			//select语句的表名放在from后面
			for i := 0; i < len(words); i++ {
				if words[i] == "from" && i != (len(words)-1) {
					table = words[i+1]
					fmt.Println("[最终可删]operation: select, table: " + table)
					break
				}
			}
		} else if words[0] == "insert" || words[0] == "delete" {
			if len(words) >= 3 {
				table = words[2]
				fmt.Println("[最终可删]operation: insert or delete, table: " + table)
			}
		} else if words[0] == "show" {
			op = SHOW
			if len(words) >= 2 {
				if words[1] == "tables" || words[1] == "indexes" {
					table = words[1]
				} else {
					fmt.Println("command show doesn't offer proper hints")
				}
			} else {
				fmt.Println("command show doesn't offer proper hints")
			}
		}
	}

	// 只要table仍为""，说明没拿到表名
	if table == "" && op <= OTHERS {
		err = errors.New("no table name in input")
	}
	if op == SHOW && table == "" {
		err = errors.New("use command show but info is inproper")
	}
	// 对于show
	return table, op, err
}

//这里目前还没有考虑没有查到ip的情况
func (client *Client) updateCache(table string) string {
	ip := ""
	// call Master.TableIP rpc
	call, err := TimeoutRPC(client.rpcMaster.Go("Master.TableIP", &table, &ip, nil), TIMEOUT)
	if err != nil {
		fmt.Println("[update cache]timeout")
		return ip
	}
	if call.Error != nil {
		fmt.Println("[table invalid]update cache failed")
		return ip
	}
	client.ipCache[table] = ip
	return ip
}
