package region

import (
	"errors"
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
			// TODO: call backup's Region.Process
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
	}
	return err
}

func (region *Region) RemoveBackup(dummyArgs, dummyReply *bool) error {
	region.backupIP = ""
	region.backupClient = nil
	return nil
}

func (region *Region) DownloadSnapshot(dummyArgs, dummyReply *bool) error {
	// TODO:
	// clean local sql dir
	// download everything from backup's sql dir to local sql dir

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
