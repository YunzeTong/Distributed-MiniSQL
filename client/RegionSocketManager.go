package client

import (
	//"bufio"
	"fmt"
	"net"
	"time"
)

type RegionSocketManager struct {
	conn net.Conn
	// sendput   *bufio.Reader
	isRunning bool

	region string
}

//类构造函数
func (rsm *RegionSocketManager) Construct() {
	rsm.region = "localhost"
	rsm.isRunning = false
}

func (rsm *RegionSocketManager) SetRegionIP(ip string) {
	rsm.region = ip
}

//与Region建立连接
//怀疑这个函数根本没用到
func (rsm *RegionSocketManager) ConnectRegionServerWithPort(PORT int) {
	rsm.isRunning = true

	fmt.Printf("新消息>>>与从节点%d建立Socket连接\n", PORT)
}

//TODO：这个要确认一下ip带没带端口号
func (rsm *RegionSocketManager) ConnectRegionServerWithIP(ip string) {
	fmt.Println("[tyz confirm] ip有无端口号：" + ip)

	// 1. 建立连接
	conn, err := net.Dial("tcp", ip)
	rsm.conn = conn
	if err != nil {
		fmt.Printf("[from client's rsm]conn server failed, err:%v\n", err)
		return
	}
	// 2. 使用conn连接进行数据的发送和接收
	// rsm.sendput = bufio.NewReader(os.Stdin)  //发送data的
	// receiveput = bufio.NewWriter() //接收data的

	rsm.isRunning = true
	//下面是尝试，直接开一个多线程，按理说应该和用内部类差不多
	go rsm.ListenToRegion()

	fmt.Printf("新消息>>>与从节点%s建立Socket连接", ip)
}

func (rsm *RegionSocketManager) ReceiveFromRegion() {
	line := ""
	if false { //TODO
		fmt.Println("新消息>>>Socket已经关闭")
	} else {
		//接收数据
		//来源https://www.cnblogs.com/yinzhengjie2020/p/12717312.html
		//https://segmentfault.com/a/1190000022734659
		var buf = make([]byte, 1024)
		n, err := rsm.conn.Read(buf)
		if err != nil {
			fmt.Printf("[client]read failed:%v\n", err)
			return
		}
		line = string(buf[:n])
	}
	if line != "" {
		fmt.Println("新消息>>>从服务器收到的信息是: " + line)
	}
}

func (rsm *RegionSocketManager) ListenToRegion() {
	fmt.Println(("新消息>>>客户端的服务器监听线程启动"))
	for rsm.isRunning {

		//socket断连的情况

		//从region接收data
		rsm.ReceiveFromRegion()

		// 风神翼龙95，但不知道是干啥的
		time.Sleep(100 * time.Millisecond)
	}
}

func (rsm *RegionSocketManager) CloseRegionConn() {
	rsm.conn.Close()
}

func (rsm *RegionSocketManager) SendToRegion(info string) {
	fmt.Println("发送给Region: " + info)

	_, err := rsm.conn.Write([]byte(info))
	if err != nil {
		fmt.Printf("[client]send failed, err:%v\n", err)
		return
	}
}
