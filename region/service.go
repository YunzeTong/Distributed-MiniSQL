package region

import (
	"fmt"
	"log"
	"net/rpc"
	"strings"

	. "Distributed-MiniSQL/common"
	"Distributed-MiniSQL/minisql/manager/api"
	"Distributed-MiniSQL/minisql/manager/interpreter"
)

func (region *Region) Process(input *string, reply *string) error {
	log.Printf("%v's Region.Process called: %v", region.hostIP, *input)
	res, err := region.processSQL(*input)
	if err != nil {
		return fmt.Errorf("%v's Region.Process failed", region.hostIP)
	} else {
		*reply = res
		if region.backupIP != "" {
			rpcBackupRegion, err := rpc.DialHTTP("tcp", region.backupIP+REGION_PORT)
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
	log.Printf("Region.AssignBackup called: backup ip: %v", *ip)
	// connect to backup
	client, err := rpc.DialHTTP("tcp", *ip+REGION_PORT)
	if err != nil {
		log.Printf("rpc.DialHTTP err: %v", *ip+REGION_PORT)
	} else {
		region.backupClient = client
		region.backupIP = *ip
		call, err := TimeoutRPC(region.backupClient.Go("Region.DownloadSnapshot", &region.hostIP, &dummyReply, nil), TIMEOUT)
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
	log.Printf("Region.RemoveBackup called: remove %v", region.backupIP)
	region.backupIP = ""
	region.backupClient = nil
	return nil
}

func (region *Region) DownloadSnapshot(ip *string, dummyReply *bool) error {
	log.Printf("Region.DownloadSnapshot called: download %v's snapshot", *ip)
	region.RemoveBackup(nil, nil)
	region.fu.DownloadDir(WORKING_DIR+DIR, DIR, *ip)
	api.Initial()
	return nil
}

func (region *Region) processSQL(sql string) (string, error) {
	res := interpreter.Interpret(sql)

	if strings.HasPrefix(res, "!") {
		return res, fmt.Errorf("process failed")
	}

	api.Store() // pending
	return res, nil
}
