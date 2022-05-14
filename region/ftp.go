package region

import (
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/jlaffaye/ftp"
)

type FtpUtils struct {
	hostIP    string //ftp的ip地址
	port      string
	username  string
	password  string
	ftpClient *ftp.ServerConn
}

func (fu *FtpUtils) Construct(masterIp string) {
	fu.hostIP = masterIp
	fu.port = "21"
	fu.username = "tyz"
	fu.password = "tyz"
}

// TODO:
// 现在的逻辑是Login完成了从连接到登录的两个操作，close完成了从退出登录到断连的操作，和风神翼龙一致
// 如果再细分的话就把连接和断连单独拿出来到构造函数里面
// 但是风神翼龙对文件的操作无一例外全都是先login再connect的，这样真的合理吗？
func (fu *FtpUtils) Login() {
	c, err := ftp.Dial(fu.hostIP+":"+fu.port, ftp.DialWithTimeout(5*time.Second))
	if err != nil {
		fmt.Printf("[from ftputils]ftp连接出现问题: %v\n", err)
	}
	err = c.Login(fu.username, fu.password)
	if err != nil {
		fmt.Printf("[from ftputils]登录出现问题: %v\n", err)
	}
	fu.ftpClient = c
}

//退出登录且断开连接
func (fu *FtpUtils) CloseConnect() {
	err := fu.ftpClient.Logout()
	if err != nil {
		fmt.Printf("[from ftpUtils]ftp退出登录出现问题: %v\n", err)
	}
	err = fu.ftpClient.Quit()
	if err != nil {
		fmt.Printf("[from ftpUtils]ftp断开连接出现问题: %v\n", err)
	}
}

/**
@param: ftpPath:ftp上路径，其下包含所要下载的文件
@param: fileName: 要下载的文件名，为""则下载ftpPath下全部文件
@param: savePath: file下载到本机的文件夹路径
*/
//风神翼龙：
//原文重载，另一个函数没有savePath参数，对应的应是catalog+IP+#+filename的形式，并下载filename
//对于这种情况，设savePath=""来标识
func (fu *FtpUtils) DownloadFile(ftpPath string, fileName string, savePath string) bool {
	fu.Login()

	//切换到工作目录
	err := fu.ftpClient.ChangeDir(ftpPath)
	if err != nil {
		fmt.Println("[from ftputils]ftpPath not exist")
		return false
	}

	//获取savePath下的所有文件的entry  https://www.serv-u.com/resource/tutorial/appe-stor-stou-retr-list-mlsd-mlst-ftp-command
	ftpFiles, e := fu.ftpClient.List("./") //TODO:个人认为前面已经设了工作目录的话这里就直接指定当前就行了，待验证
	if e != nil {
		fmt.Printf("[from ftputils]ftpfiles list fail: %v\n", e)
		return false
	}
	if ftpFiles == nil || len(ftpFiles) == 0 {
		fmt.Println("[from ftputils]list下无文件")
		return false
	}

	// 该循环的含义：
	// fileName == "": 获取ftpPath下所有非文件夹文件并下载
	// fileName != "": 获取ftpPath下名为fileName的文件并下载
	for _, file := range ftpFiles {
		if fileName == "" || file.Name == fileName {
			if file.Type.String() != "folder" { //不为folder即下载，F12查看函数签名，但又有一个link不知道是什么
				//打开本地文件
				var localfile *os.File
				if savePath == "" { //原文重载的区别之处
					localfile, _ = os.OpenFile(strings.Split(file.Name, "#")[1], os.O_RDWR|os.O_CREATE, 0777)
				} else {
					localfile, _ = os.OpenFile(savePath+file.Name, os.O_RDWR|os.O_CREATE, 0777)
				}
				defer localfile.Close()
				//获取ftp文件
				fetchfile, _ := fu.ftpClient.Retr(file.Name)
				defer fetchfile.Close()
				//复制
				io.Copy(localfile, fetchfile)
			}
		}
	}
	fu.CloseConnect()
	return true
}

//风神翼龙：重载二合一
// IP为""则执行两个参数的，否则执行三个参数的，唯一区别为带IP的更改了fileName
// fileName: 要上传的文件名
// savePath: ftp上文件存在的文件夹路径
// IP: ...不知道是啥
func (fu *FtpUtils) UploadFile(fileName string, savePath string, IP string) bool {
	fu.Login()
	//根据savepath建立文件夹
	err := fu.ftpClient.MakeDir(savePath)
	if err != nil {
		fmt.Printf("[from ftputils]make dir failed: %v\n", err)
		return false
	}
	//切换到当前工作目录
	err = fu.ftpClient.ChangeDir(savePath)
	if err != nil {
		fmt.Printf("[from ftputils]change dir failed: %v\n", err)
		return false
	}
	//先读取本地文件，https://www.codeleading.com/article/96605360211/
	var file *os.File
	file, err = os.Open(fileName)
	if err != nil {
		fmt.Printf("[from ftputils]read file failed: %v\n", err)
		return false
	}
	defer file.Close()
	//上传文件
	err = fu.ftpClient.Stor(fileName, file)
	if err != nil {
		fmt.Printf("[from ftputils]uploading file failed: %v\n", err)
		return false
	}
	if IP != "" {
		err = fu.ftpClient.Rename(fileName, "/catalog/"+IP+"#"+fileName)
		if err != nil {
			fmt.Printf("[from ftputils]rename file failed: %v\n", err)
			return false
		}
	}
	fu.CloseConnect()
	return true
}

// filePath:ftp上文件路径
// fileName:要删的ftp上文件名
func (fu *FtpUtils) DeleteFile(fileName string, filePath string) bool {
	fu.Login()
	fu.ftpClient.ChangeDir(filePath)
	err := fu.ftpClient.Delete(fileName)
	if err != nil {
		fmt.Printf("[from ftputils]delete file failed: %v\n", err)
		return false
	}

	fu.CloseConnect()
	return true
}
