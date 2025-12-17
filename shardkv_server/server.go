package main

import (
	"flag"

	"github.com/khyallin/shardkv/config"
	"github.com/khyallin/shardkv/group"
	"github.com/khyallin/shardkv/model"
	"github.com/khyallin/shardkv/rpc"
)

func main() {
	cfg := config.DefaultConfig()
	svr := rpc.NewServer()
	gid := flag.Int("gid", -1, "-gid <gid>")
	me := flag.Int("me", -1, "-me <me>")
	flag.Parse()
	if *gid == -1 || *me == -1 {
		panic("gid & me must be specified")
	}
	kv, rf := group.MakeKVServer(cfg.Groups[model.Tgid(*gid)], model.Tgid(*gid), *me, config.Maxraftstate)
	svr.Register("KVServer", kv)
	svr.Register("Raft", rf)
	svr.Start()
	select {}
}
