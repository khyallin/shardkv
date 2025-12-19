package rpc

import (
	"github.com/khyallin/shardkv/api"
	"github.com/khyallin/shardkv/internal/config"
)

type FreezeShardArgs struct {
	Shard config.Tshid
	Num   config.Tnum
}

type FreezeShardReply struct {
	State []byte
	Num   config.Tnum
	Err   api.Err
}

type InstallShardArgs struct {
	Shard config.Tshid
	State []byte
	Num   config.Tnum
}

type InstallShardReply struct {
	Err api.Err
}

type DeleteShardArgs struct {
	Shard config.Tshid
	Num   config.Tnum
}

type DeleteShardReply struct {
	Err api.Err
}
