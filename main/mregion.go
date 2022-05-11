package main

import (
	. "Distributed-MiniSQL/region"
	"os"
)

func main() {
	var region Region
	region.Init(os.Args[1], os.Args[2])

	region.Run()
}
