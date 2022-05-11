package main

import . "Distributed-MiniSQL/client"

func main() {
	var client Client
	client.Init()
	client.Run()
}
