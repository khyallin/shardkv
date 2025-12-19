package main

import (
	"flag"
	"io"
	"log"
	"os"
)

func main() {
	mode := flag.String("mode", "server", "mode: server/ctrler/client")
	flag.Parse()

	if os.Getenv("DEBUG") != "1" {
		log.SetOutput(io.Discard)
	}

	switch *mode {
	case "server":
		startServer()
	case "ctrler":
		startCtrler()
	case "client":
		startClient()
	default:
		log.Fatal("未知模式: " + *mode)
	}
}
