package master

import (
	"math"

	. "Distributed-MiniSQL/common"
)

func (master *Master) serverExists(ip string) bool {
	_, ok := master.servers[ip]
	return ok
}

func (master *Master) addServer(ip string, tables []string) {
	master.servers[ip] = true
	temp := make([]string, 0)
	master.serverTables[ip] = &temp
	for _, table := range tables {
		master.addTable(ip, table)
	}
}

func (master *Master) addTable(table, ip string) {
	// guaranteed that addServer(ip) is called
	master.tableLoc[table] = ip
	AddUniqueToSlice(master.serverTables[ip], table)
}

func (master *Master) deleteTable(table, ip string) {
	delete(master.tableLoc, table)
	DeleteFromSlice(master.serverTables[ip], table)
}

func (master *Master) bestServer(excluded string) string {
	min, res := math.MaxInt, ""
	for ip, pTables := range master.serverTables {
		if ip != excluded && len(*pTables) < min {
			min, res = len(*pTables), ip
		}
	}
	return res
}

func (master *Master) tableLocation(table string) string {
	return master.tableLoc[table]
}

func (master *Master) transferServerTables(src, dst string) {
	pTables := master.serverTables[src]
	for _, tab := range *pTables {
		master.tableLoc[tab] = dst
		master.addTable(tab, dst)
	}

	delete(master.serverTables, src)
}
