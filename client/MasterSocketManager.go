package client

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

type MasterSocketManager struct {
	conn      net.Conn
	reader    *bufio.Reader
	isRunning bool

	clientManager *ClientManager

	// 服务器的IP和端口号
	master string
	PORT   int

	// 使用map来存储需要处理的表名-sql语句的对应关系
	commandMap map[string]string
}

func (msm *MasterSocketManager) Construct(cm *ClientManager) {
	// commandmap初始化
	msm.commandMap = make(map[string]string)
	msm.PORT = 12345
	msm.master = "localhost:12345" //在这里为了net.Dial直接传参多加了端口号
	msm.clientManager = cm

	conn, err := net.Dial("tcp", msm.master)
	msm.conn = conn
	if err != nil {
		fmt.Printf("[from client's msm]conn server failed, err:%v\n", err)
		return
	}

	readput := bufio.NewReader(conn)
	msm.reader = readput
	msm.isRunning = true

	//开启监听线程
	go msm.ListenToMaster()
}

// 向主服务器发送信息的api
// 要加上client标签，可以被主服务器识别
func (msm *MasterSocketManager) SendToMaster(info string) {
	sendContent := "<client>[1]" + info

	_, err := msm.conn.Write([]byte(sendContent))
	if err != nil {
		fmt.Printf("[from client's msm]send failed, err:%v\n", err)
		return
	}
}

func (msm *MasterSocketManager) SendToMasterCreate(info string) {
	sendContent := "<client>[2]" + info

	_, err := msm.conn.Write([]byte(sendContent))
	if err != nil {
		fmt.Printf("[from client's msm]send failed, err:%v\n", err)
		return
	}
}

// 接收来自master server的信息并显示
// 新增代码，查询主服务器中存储的表名和对应的端口号
// 主服务器返回的内容的格式应该是"<table>table port"，因此args[0]和[1]分别代表了表名和对应的端口号
func (msm *MasterSocketManager) ReceiveFromMaster() {
	line := ""

	if false { //TODO:判断socket断连
		fmt.Println("新消息>>>Socket已经关闭!")
	} else {
		linebuf, _, err := msm.reader.ReadLine()
		if err != nil {
			fmt.Printf("[from client's msm]read from conn failed, err:%v\n", err)
		}
		line = string(linebuf)
	}
	if line != "" {
		fmt.Println("新消息>>>从服务器收到的信息是: " + line)
		// 已经废弃的方法？风神翼龙63
		if strings.HasPrefix(line, "<table>") {
			args := strings.Split(line[7:], " ")
			sql, ok := msm.commandMap[args[0]]
			//如果查到的端口号有对应的表
			if ok {
				//不是很懂为什么非要用int
				//PORT := args[1]
				PORT, _ := strconv.Atoi(args[1])
				fmt.Println(sql)
				delete(msm.commandMap, args[0])
				//查询到之后在client的cache中设置一个缓存
				msm.clientManager.cacheManager.SetCache(args[0], args[1]) //args[1]: str类型PORT
				msm.clientManager.ConnectToRegionWithPort(PORT, sql)
			}
		} else if strings.HasPrefix(line, "<master>[1]") || strings.HasPrefix(line, "<master>[2]") {
			//截取ip地址
			args := strings.Split(line[11:], " ")
			ip, table := args[0], args[1]
			msm.clientManager.cacheManager.SetCache(table, ip)
			msm.clientManager.ConnectToRegionWithIP(ip, msm.commandMap[table])
		}
	}
}

// 将sql语句发送到主服务器进一步处理，这里还有待进一步开发，目前仅供实验
// 进一步开发在这个方法里面扩展

func (msm *MasterSocketManager) Process(sql string, table string) {
	// 来处理sql语句
	msm.commandMap[table] = sql
	// 用<table>前缀表示要查某个表名对应的端口号
	msm.SendToMaster(table)
}

func (msm *MasterSocketManager) ProcessCreate(sql string, table string) {
	msm.commandMap[table] = sql
	// 用<table>前缀表示要查某个表名对应的端口号
	fmt.Println("存入table的是" + table + " " + sql)
	msm.SendToMasterCreate(table)
}

// 关闭socket的方法，在输入quit的时候直接调用
func (msm *MasterSocketManager) CloseMasterConn() {
	msm.conn.Close()
}

func (msm *MasterSocketManager) ListenToMaster() {
	fmt.Println("新消息>>>客户端的主服务器监听线程启动！")
	for msm.isRunning {
		if false {
			//TODO 如果conn挂掉了
			msm.isRunning = false
			break
		}

		msm.ReceiveFromMaster()

		time.Sleep(100 * time.Millisecond)

	}
}
