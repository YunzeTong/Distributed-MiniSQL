package main

import (
	. "Distributed-MiniSQL/client"
	"os"
)

func main() {
	var client Client
	client.Init(os.Args[1])
	client.Run()
}
