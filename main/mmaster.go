package main

import . "Distributed-MiniSQL/master"

func main() {
	var master Master
	master.Init()
	master.Serve()
}
