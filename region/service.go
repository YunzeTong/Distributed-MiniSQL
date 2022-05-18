package region

import (
	"errors"
	"fmt"
	"log"
	"net/rpc"

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
	region.fu.DownloadDir(DIR, DIR, *ip)
	api.Initial()
	return nil
}

func (region *Region) processSQL(sql string) (string, error) {
	res := interpreter.Interpret(sql)

	// pending
	if res == "fail" {
		return "", errors.New("fail")
	}

	api.Store() // pending
	return res, nil
}
