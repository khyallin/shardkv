package main

import (
	"fmt"

	"github.com/khyallin/shardkv/config"
	"github.com/khyallin/shardkv/controller"
)

var defaultConfig = &config.Config{
	Num:    1,
	Shards: [config.NShards]config.Tgid{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
	Groups: map[config.Tgid][]string{
		1: {"shardkv-server-1-0", "shardkv-server-1-1", "shardkv-server-1-2"},
	},
}

func startCtrler() {
	if env.Servers == nil || len(env.Servers) == 0 {
		fmt.Println("Usage: shardkv ctrler -servers <server1,server2,...>")
		return
	}

	ctrler := controller.MakeController(env.Servers)
	ctrler.InitConfig(defaultConfig)
}
