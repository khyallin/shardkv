package main

import (
	"fmt"
)

func main() {
	if env.Mode == "" {
		fmt.Println("Usage: shardkv <mode> [args]")
		fmt.Println("Use 'shardkv help' for more information.")
	}
	switch env.Mode {
	case "help":
		fmt.Println("Usage: shardkv <mode> [args]")
		fmt.Println("Modes:")
		fmt.Println("  server   Start a shardkv server")
		fmt.Println("  ctrler   Start a shardkv controller")
		fmt.Println("  client   Start a shardkv client")
	case "server":
		startServer()
	case "ctrler":
		startCtrler()
	case "client":
		startClient()
	default:
		fmt.Println("Unknown mode: " + env.Mode)
	}
}
