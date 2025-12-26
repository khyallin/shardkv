package main

import (
	"fmt"

	"github.com/khyallin/shardkv/config"
	"github.com/khyallin/shardkv/internal/group"
	"github.com/khyallin/shardkv/internal/rpc"
)

func startServer() {
	if env.GroupId == -1 || env.Me == -1 || env.Servers == nil || len(env.Servers) == 0 {
		fmt.Println("Usage: shardkv server -gid <groupid> -me <serverindex> -servers <server1,server2,...>")
		return
	}
	rpcServer()
}

func rpcServer() {
	svr := rpc.NewServer()
	kv, rf := group.MakeKVServer(env.Servers, env.GroupId, env.Me, config.Maxraftstate)
	svr.Register("KVServer", kv)
	svr.Register("Raft", rf)
	svr.Start()
	select {}
}
