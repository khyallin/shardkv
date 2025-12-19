package main

import (
	"encoding/json"
	"log"
	"os"
	"strconv"

	"github.com/khyallin/shardkv/internal/config"
	"github.com/khyallin/shardkv/internal/group"
	"github.com/khyallin/shardkv/internal/rpc"
)

func startServer() {
	gid, me, svrs := getArgs()

	svr := rpc.NewServer()
	_ = group.MakeKVServer(svr, svrs, gid, me, config.Maxraftstate)
	svr.Start()
}

func getArgs() (config.Tgid, int, []string) {
	gid, err := strconv.Atoi(os.Getenv("GID"))
	if err != nil {
		log.Fatal("Invalid GID")
	}
	me, err := strconv.Atoi(os.Getenv("ME"))
	if err != nil {
		log.Fatal("Invalid ME")
	}
	svrs := os.Getenv("SVRS")
	if svrs == "" {
		return config.Tgid(gid), me, []string{}
	}

	var s []string
	if err := json.Unmarshal([]byte(svrs), &s); err != nil {
		log.Fatalf("invalid SVRS: %v, raw=%q", err, svrs)
	}
	return config.Tgid(gid), me, s
}
