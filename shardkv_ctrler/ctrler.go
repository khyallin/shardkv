package main

import (
	"github.com/khyallin/shardkv/config"
	"github.com/khyallin/shardkv/controller"
)

func main() {
	ctrler := controller.MakeController()
	cfg := config.DefaultConfig()
	ctrler.InitConfig(cfg)
}
