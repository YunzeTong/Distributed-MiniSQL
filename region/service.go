package region

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path"

	. "Distributed-MiniSQL/common"
	"Distributed-MiniSQL/minisql/manager/api"
	"Distributed-MiniSQL/minisql/manager/interpreter"
)

func (region *Region) Process(input *string, reply *string) error {
	log.Printf("%v: Region.Process called: %v", region.hostIP, *input)

	res, err := region.processSQL(*input)
	if err != nil {
		return errors.New("syntax error")
	} else {
		*reply = res
		if region.backupIP != "" {
			// TODO: call backup's Region.Process
			rpcBackupRegion, err := rpc.DialHTTP("tcp", region.backupIP)
			if err != nil {
				fmt.Println("fail to connect to backup region: " + region.backupIP)
			}
			call, err := TimeoutRPC(rpcBackupRegion.Go("Region.Process", &input, &reply, nil), TIMEOUT)
			if err != nil {
				fmt.Println("timeout")
			}
			if call.Error != nil {
				fmt.Println("[backup region] failed")
			}
		}
	}
	return err
}

func (region *Region) AssignBackup(ip *string, dummyReply *bool) error {
	// connect to backup
	client, err := rpc.DialHTTP("tcp", region.backupIP+REGION_PORT)
	if err != nil {
		log.Printf("rpc.DialHTTP err: %v", region.backupIP+REGION_PORT)
	} else {
		region.backupClient = client
		region.backupIP = *ip
		// TODO: call backup's Region.DownloadSnapshot
		call, err := TimeoutRPC(region.backupClient.Go("Region.DownloadSnapshot", &region.backupIP, &dummyReply, nil), TIMEOUT)
		if err != nil {
			fmt.Println("timeout")
			return err
		}
		if call.Error != nil {
			fmt.Println("[backup region's downloadSnapshot] failed")
			return call.Error
		}
		return nil
	}
	return err
}

func (region *Region) RemoveBackup(dummyArgs, dummyReply *bool) error {
	region.backupIP = ""
	region.backupClient = nil
	return nil
}

func (region *Region) DownloadSnapshot(ip *string, dummyReply *bool) error {
	// TODO:
	// clean local sql dir
	dir, err := ioutil.ReadDir("./sql")
	if err != nil {
		fmt.Println("Can't obtain files in ./sql")
	}
	for _, d := range dir {
		os.RemoveAll(path.Join([]string{"sql", d.Name()}...))
	}
	// download everything from backup's sql dir to local sql dir
	region.fu.Login(*ip)
	//切换到工作目录
	err = region.fu.ftpClient.ChangeDir("./sql/")
	if err != nil {
		fmt.Println("[from ftputils]ftpPath not exist")
		return err
	}
	//获取savePath下的所有文件的entry  https://www.serv-u.com/resource/tutorial/appe-stor-stou-retr-list-mlsd-mlst-ftp-command
	ftpFiles, e := region.fu.ftpClient.List("./") //TODO:个人认为前面已经设了工作目录的话这里就直接指定当前就行了，待验证
	if e != nil {
		fmt.Printf("[from backup ftp]ftpfiles list fail: %v\n", e)
		return e
	}
	if ftpFiles == nil {
		fmt.Println("[from backup ftp]list下无文件")
		return e
	}
	for _, file := range ftpFiles {
		//打开sql文件夹里的本地文件
		var localfile *os.File
		localfile, _ = os.OpenFile("./sql/"+file.Name, os.O_RDWR|os.O_CREATE, 0777)
		defer localfile.Close()
		//获取ftp文件
		fetchfile, _ := region.fu.ftpClient.Retr(file.Name)
		defer fetchfile.Close()
		//复制
		io.Copy(localfile, fetchfile)

	}
	region.fu.CloseConnect()

	api.Initial()

	return nil
}

// I know it looks dirty, just avoid premature optimization
func (region *Region) processSQL(sql string) (string, error) {
	res := interpreter.Interpret(sql)

	// pending
	if res == "fail" {
		return "", errors.New("fail")
	}

	api.Store() // pending
	return res, nil
}
