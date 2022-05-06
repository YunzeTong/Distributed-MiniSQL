package client

import (
	"fmt"
	"strings"
	"time"
)

type ClientManager struct {
	cacheManager        CacheManager
	masterSocketManager MasterSocketManager
	regionSocketManager RegionSocketManager
}

func (cm *ClientManager) Construct() {
	cm.cacheManager.Construct()
	cm.masterSocketManager.Construct(cm)
	cm.regionSocketManager.Construct()
}

func (cm *ClientManager) Run() {
	fmt.Println("start")

	line := ""

	for {

		sql := ""
		// 读入一句完整的SQL语句
		fmt.Println("新消息>>>请输入你想要执行的SQL语句：")

		for len(line) == 0 || line[len(line)-1] != ';' {
			fmt.Scanln(&line)
			if len(line) == 0 {
				continue
			}
			sql += line
			sql += " "
		}

		line = ""
		fmt.Println(sql)
		sql = strings.TrimSpace(sql)

		if sql == "quit;" {
			cm.masterSocketManager.CloseMasterConn()
			if cm.regionSocketManager.conn != nil {
				cm.regionSocketManager.CloseRegionConn()
			}
			break
		}

		// 获得目标表名和索引名
		// 风神翼龙59
		command := sql
		sql = ""
		target := cm.Interpreter(command)
		_, hasKey := target["error"]
		if hasKey {
			fmt.Println(("新消息>>>输入有误，请重试"))
		}

		// 风神翼龙67
		table := target["name"]
		cache := ""
		fmt.Println(("新消息>>>需要处理的表名: " + table))

		if target["kind"] == "create" {
			cm.masterSocketManager.ProcessCreate(command, table)
		} else {
			if target["cache"] == "true" {
				cache = cm.cacheManager.GetCache(table)
				if len(cache) == 0 {
					fmt.Println("新消息>>>客户端缓存中不存在该表")
				} else {
					fmt.Println(("新消息>>>客户端缓存中存在该表！其对应的服务器是：" + cache))
				}
			}

			// 如果cache里面没有找到表所对应的端口号，那么就去masterSocket里面查询
			// 风神翼龙83
			if len(cache) == 0 {
				cm.masterSocketManager.Process(command, table)
			} else {
				// 如果查到了端口号就直接在RegionSocketManager中进行连接
				// TODO: 类型不一致，ip是string, 端口是int，待仔细查看
				cm.ConnectToRegionWithIP(cache, command)
			}
		}
	}
}

// 和从节点建立连接并发送SQL语句过去收到执行结果
// 风神翼龙94
func (cm *ClientManager) ConnectToRegionWithPort(PORT int, sql string) {
	cm.regionSocketManager.ConnectRegionServerWithPort(PORT)
	time.Sleep(100 * time.Millisecond)
	cm.regionSocketManager.SendToRegion(sql)
}

// 类重载方法，使用ip地址进行连接，端口号固定为22222
// 风神翼龙101
func (cm *ClientManager) ConnectToRegionWithIP(ip string, sql string) {
	cm.regionSocketManager.ConnectRegionServerWithIP(ip)
	time.Sleep(100 * time.Millisecond)
	cm.regionSocketManager.SendToRegion(sql)
}

//风神翼龙108
// TODO: 返回类型不确定是*引用还是值，先这么写
func (cm *ClientManager) Interpreter(sql string) map[string]string {
	//粗略解析需要操作的table和index的名字
	result := map[string]string{} //声明并初始化
	result["cache"] = "true"
	//空格替换
	sql = strings.ReplaceAll(sql, "\\s+", " ")
	words := strings.Split(sql, " ")
	//SQL语句的种类
	result["kind"] = words[0]
	if words[0] == "create" {
		// 对应create table xxx和create index xxx
		// 此时创建新表，不需要cache
		result["cache"] = "false"
		result["name"] = words[2]
	} else if words[0] == "drop" || words[0] == "insert" || words[0] == "delete" {
		// 这三种都是将table和index放在第三个位置的，可以直接取出
		name := strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(words[2], "(", ""), ")", ""), ";", "")
		result["name"] = name
	} else if words[0] == "select" {
		//select语句的表名放在from后面
		for i := 0; i < len(words); i++ {
			if words[i] == "from" && i != (len(words)-1) {
				result["name"] = words[i+1]
				break
			}
		}
	}
	// 如果没有发现表名就说明出出现错误
	_, hasKeyname := result["name"]
	if !hasKeyname {
		result["error"] = "true"
	}
	return result

}
