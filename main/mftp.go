package main

import (
	. "Distributed-MiniSQL/region"
	"fmt"
	"os"
)

func main() {
	var ftp FtpUtils
	// test
	localFileName := "test.txt"
	remoteFileName := "tes.txt"
	newLocalFileName := "te.txt"
	// clientPath := "./rubbishfile/"
	ftp.Construct(os.Args[1])
	fmt.Println("start to upload")
	ftp.UploadFile(localFileName, remoteFileName) //假定当前路径minisql，将test.txt传入到  /home/tyz
	fmt.Println("upload test finished")
	fmt.Println("start to download file")
	ftp.DownloadFile(remoteFileName, newLocalFileName, false)
	fmt.Println("finish downloading")
	fmt.Println("start to delete file")
	ftp.DeleteFile(remoteFileName) // 删除/home/tyz下的test.txt
	fmt.Println("finish deleting")
}
