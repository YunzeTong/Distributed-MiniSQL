package client

import "fmt"

type CacheManager struct {
	cache map[string]string
}

func (cm *CacheManager) Construct() {
	cm.cache = make(map[string]string)
}

/*
* 查询某张表是否存在客户端中，如果存在就直接返回表名
* @param table 要查询的表名
 */
func (cm *CacheManager) GetCache(table string) string {
	_, ok := cm.cache[table]
	if ok {
		return cm.cache[table]
	}
	return ""
}

/**
* 在客户端缓存中存储已知的表和所在的服务器
* @param table 数据表的名称
* @param server 服务器的IP地址和端口号
 */
func (cm *CacheManager) SetCache(table string, server string) {
	cm.cache[table] = server
	fmt.Println("存入缓存：表名" + table + " 端口号：" + table)
}
