package main

import (
	. "Distributed-MiniSQL/master"
	"os"
	"strconv"
)

func main() {
	var master Master
	regionCount, _ := strconv.ParseInt(os.Args[1], 10, 0)
	master.Init(int(regionCount))
	master.Run()
}
