package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/khyallin/shardkv/api"
	"github.com/khyallin/shardkv/client"
)

type Client struct {
	clerk *client.Clerk
}

func NewClient(servers []string) *Client {
	return &Client{
		clerk: client.MakeClerk(servers),
	}
}

func (c *Client) Put(args []string) {
	if len(args) < 2 {
		fmt.Println("Usage: put <key> <value> [version]")
		return
	}
	key := args[0]
	value := args[1]
	version := 0
	if len(args) >= 3 {
		fmt.Sscanf(args[2], "%d", &version)
	}

	err := c.clerk.Put(key, value, api.Tversion(version))
	if err != api.OK {
		fmt.Println("Put Fail:", err)
		return
	}
	fmt.Printf("Put key=%s, value=%s, version=%d\n", key, value, version)
}

func (c *Client) Get(args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: get <key>")
		return
	}
	key := args[0]
	value, version, err := c.clerk.Get(key)
	if err != api.OK {
		fmt.Println("Get Fail:", err)
		return
	}
	fmt.Printf("Get key=%s, value=%s, version=%d\n", key, value, version)
}

func startClient() {
	if env.Servers == nil || len(env.Servers) == 0 {
		fmt.Println("Usage: shardkv client -servers <server1,server2,...>")
		return
	}

	client := NewClient(env.Servers)
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Println("Entering interactive mode, type put/get commands, exit to quit:")
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			fmt.Println("\nExited")
			break
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if line == "exit" {
			fmt.Println("Exited")
			break
		}
		parts := strings.Fields(line)
		cmd := strings.ToLower(parts[0])
		switch cmd {
		case "put":
			client.Put(parts[1:])
		case "get":
			client.Get(parts[1:])
		default:
			fmt.Println("Unknown command: ", cmd)
		}
	}
}
