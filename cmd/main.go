package main

import (
	"flag"
	"log"
)

func main() {
	flag.Parse()
	args := flag.Args()
	if len(args) < 1 {
		log.Fatal("Use 'shardkv help' for usage information.")
	}
	mode := args[0]
	modeArgs := args[1:]

	switch mode {
	case "help":
		log.Println("Usage: shardkv <mode> [args]")
		log.Println("Modes:")
		log.Println("  server   Start a shardkv server")
		log.Println("  ctrler   Start a shardkv controller")
		log.Println("  client   Start a shardkv client")
	case "server":
		startServer(modeArgs)
	case "ctrler":
		startCtrler(modeArgs)
	case "client":
		startClient(modeArgs)
	default:
		log.Fatal("Unknown mode: " + mode)
	}
}
