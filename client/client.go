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
	CREATE_TBL = 0
	DROP_TBL   = 1
	SHOW_TBL   = 2
	CREATE_IDX = 3
	DROP_IDX   = 4
	SHOW_IDX   = 5
	OTHERS     = 6
)

func (client *Client) Init(masterIP string) {
	client.ipCache = make(map[string]string)
	client.rpcRegionMap = make(map[string]*rpc.Client)

	rpcMas, err := rpc.DialHTTP("tcp", masterIP+MASTER_PORT)
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
			//客户端直接退出
			break
		}

		op, table, index, err := client.preprocessInput(input)
		if err != nil {
			fmt.Printf("[input format error] %v\n", err)
			continue
		}
		switch op {
		case CREATE_TBL:
			// call Master.CreateTable rpc
			args, ip := TableArgs{Table: table, SQL: input}, ""
			call, err := TimeoutRPC(client.rpcMaster.Go("Master.CreateTable", &args, &ip, nil), TIMEOUT)
			if err != nil {
				fmt.Println("timeout")
			}
			if call.Error != nil {
				fmt.Println("[from master to client]create table failed")
			} else {
				fmt.Println("create table succeed, table in ip: " + ip)
			}
		case DROP_TBL:
			// call Master.DropTable rpc
			args, dummy := TableArgs{Table: table, SQL: input}, false
			call, err := TimeoutRPC(client.rpcMaster.Go("Master.DropTable", &args, &dummy, nil), TIMEOUT)
			if err != nil {
				fmt.Println("timeout")
			}
			if call.Error != nil {
				fmt.Println("[error]drop table failed")
			}
		case SHOW_TBL:
			// 把所有region的table名显示出来
			// TODO: call Master.ShowTables and format output
			fmt.Println("show all the tables in region")
			var dummyArgs bool
			var tables []string
			// tables = make([]string, 0)
			call, err := TimeoutRPC(client.rpcMaster.Go("Master.ShowTables", &dummyArgs, &tables, nil), TIMEOUT)
			if err != nil {
				fmt.Println("timeout")
			}
			if call.Error != nil {
				fmt.Println("[error]show tables failed")
			} else {
				fmt.Println("tables in region:\n---------------")
				for _, table := range tables {
					fmt.Printf("|  %v  |\n", table)
				}
				fmt.Println("---------------")
			}

		case CREATE_IDX:
			// TODO: call Master.CreateIndex
			// TODO: 返回的ip是要update tableip map吗？
			args, ip := IndexArgs{Index: index, Table: table, SQL: input}, ""
			call, err := TimeoutRPC(client.rpcMaster.Go("Master.CreateIndex", &args, &ip, nil), TIMEOUT)
			if err != nil {
				fmt.Println("timeout")
			}
			if call.Error != nil {
				fmt.Println("[error]create indexes failed")
			} else {
				fmt.Printf("create index succeed on table %v in %v\n", table, ip)
			}
		case DROP_IDX:
			// TODO: call Master.DropIndex
			args, dummy := IndexArgs{Index: index, SQL: input}, false
			call, err := TimeoutRPC(client.rpcMaster.Go("Master.DropIndex", &args, &dummy, nil), TIMEOUT)
			if err != nil {
				fmt.Println("timeout")
			}
			if call.Error != nil {
				fmt.Println("[error]drop indexes failed")
			} else {
				fmt.Println("drop index succeed")
			}
		case SHOW_IDX:
			// TODO: call Master.ShowIndices and format output
			fmt.Println("show all the indexes in region")
			var dummyArgs bool
			var indices map[string]string
			// indices = make(map[string]string)
			call, err := TimeoutRPC(client.rpcMaster.Go("Master.ShowIndices", &dummyArgs, &indices, nil), TIMEOUT)
			if err != nil {
				fmt.Println("timeout")
			}
			if call.Error != nil {
				fmt.Println("[error]show indices failed")
			} else {
				fmt.Println("indices in region:\n|  index  |  table  |")
				for index, table := range indices {
					fmt.Printf("|  %v  |  %v  |\n", index, table)
				}
				fmt.Println("---------------")
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
				rpcRegion, err = rpc.DialHTTP("tcp", ip+REGION_PORT)
				if err != nil {
					fmt.Println(err)
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
				new_rpcRegion, err := rpc.DialHTTP("tcp", new_ip+REGION_PORT)
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
				fmt.Println("result:\n" + result)
				client.ipCache[table] = new_ip
				client.rpcRegionMap[new_ip] = new_rpcRegion
				fmt.Println("[最终不一定非得删除]update ip: " + ip + " and add " + new_ip + " to iptablemap")
			}
			fmt.Println("result:\n" + result)
		}

	}
}

// create格式默认正确写法: create table student (name varchar, id int);

func (client *Client) preprocessInput(input string) (TableOp, string, string, error) {
	input = strings.Trim(input, ";")
	//初始化四个返回值
	table := ""
	index := ""
	var op TableOp
	op = OTHERS
	var err error
	err = nil
	//空格替换
	input = strings.ReplaceAll(input, "\\s+", " ")
	words := strings.Split(input, " ")
	if words[0] == "create" {
		if len(words) < 3 {
			//无论哪一种create都必须要有3个及以上word
			err = errors.New("lack of input in create operation")
			return op, table, index, err
		}

		if words[1] == "table" {
			op = CREATE_TBL
			table = words[2]
			if strings.Contains(table, "(") { // 如果被划分成了 student(name varchar, ...)
				table = table[0:strings.Index(table, "(")]
			}
			fmt.Println("[最终可删除]table:" + table)
		} else if words[1] == "index" {
			op = CREATE_IDX
			if len(words) < 5 {
				err = errors.New("lack of words in create index")
				return op, table, index, err
			}
			index = words[2]
			fmt.Println("[最终可删除]index:" + index)
			table = words[4]
			if strings.Contains(table, "(") { // 如果被划分成了 student(name)
				table = table[0:strings.Index(table, "(")]
			}
			fmt.Println("[最终可删除]table:" + table)
		} else {
			err = errors.New("the type you create can't be recognized")
		}
		return op, table, index, err
	} else if words[0] == "drop" {
		if len(words) == 3 {
			if words[1] == "table" {
				op = DROP_TBL
				table = words[2]
			} else if words[1] == "index" {
				op = DROP_IDX
				index = words[2]
			} else {
				err = errors.New("please drop table or index")
			}
		} else {
			err = errors.New("number of words false in drop")
		}
		return op, table, index, err
	} else if words[0] == "show" {
		if len(words) == 2 {
			if words[1] == "tables" {
				op = SHOW_TBL
			} else if words[1] == "indexes" {
				op = SHOW_IDX
			} else {
				err = errors.New("show command only specify indexes/tables")
			}
			return op, table, index, err
		} else {
			err = errors.New("show command only need to specify indexes/tables")
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
		} else {
			err = errors.New("can't recoginize your command")
		}
	}

	// 只要table仍为""，说明没拿到表名
	if table == "" && op == OTHERS && err == nil {
		err = errors.New("no table name in input")
	}
	return op, table, index, err
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
