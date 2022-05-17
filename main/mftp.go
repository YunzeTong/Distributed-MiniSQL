package main

import (
	. "Distributed-MiniSQL/region"
	"fmt"
	"os"
)

func main() {
	var ftp FtpUtils
	// test
	fileName := "test.txt"
	// clientPath := "./rubbishfile/"
	ftpPath := "/home/tyz/"
	ftp.Construct(os.Args[1])
	fmt.Println("start to upload")
	ftp.UploadFile(fileName, ftpPath, "") //假定当前路径minisql，将test.txt传入到  /home/tyz
	fmt.Println("upload test finished")
	fmt.Println("start to download file")
	ftp.DownloadFile("./", fileName, "./main/")
	fmt.Println("finish downloading")
	fmt.Println("start to delete file")
	ftp.DeleteFile(fileName, "./") // 删除/home/tyz下的test.txt
	fmt.Println("finish deleting")
}
