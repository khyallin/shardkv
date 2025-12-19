package main

import "github.com/khyallin/shardkv/client"

func startClient() {
	clerk := client.MakeClerk()
	clerk.Put("key", "value", 0)
	value, version, _ := clerk.Get("key")
	println("Got value:", value, "with version:", version)
}
