package main

import (
	"github.com/khyallin/shardkv/internal/config"
	"github.com/khyallin/shardkv/internal/controller"
)

func startCtrler() {
	ctrler := controller.MakeController()
	cfg := config.DefaultConfig()
	ctrler.InitConfig(cfg)
}