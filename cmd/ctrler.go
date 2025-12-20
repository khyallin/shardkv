package main

import (
	"fmt"
	"io"
	"log"
	"os"

	"github.com/khyallin/shardkv/config"
	"github.com/khyallin/shardkv/controller"
)

var defaultConfig = &config.Config{
	Num:    1,
	Shards: [config.NShards]config.Tgid{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
	Groups: map[config.Tgid][]string{
		0: {"172.20.0.10", "172.20.0.11", "172.20.0.12"},
		1: {"172.20.0.13", "172.20.0.14", "172.20.0.15"},
	},
}

func startCtrler(args []string) {
	if os.Getenv("DEBUG") != "1" {
		log.SetOutput(io.Discard)
	}
	if len(args) < 1 {
		fmt.Printf("Usage: shardkv ctrler <server1> <server2> ...")
		return
	}

	servers := defaultConfig.Groups[0]
	ctrler := controller.MakeController(servers)
	ctrler.InitConfig(defaultConfig)
}
