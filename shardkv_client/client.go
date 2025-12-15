package main

import "github.com/khyallin/shardkv/clerk"

func main() {
	clerk := clerk.MakeClerk()
	clerk.Put("key", "value", 0)
	value, version, _ := clerk.Get("key")
	println("Got value:", value, "with version:", version)
}
