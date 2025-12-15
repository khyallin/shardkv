package rpc

import "github.com/khyallin/shardkv/model"

type FreezeShardArgs struct {
	Shard model.Tshid
	Num   model.Tnum
}

type FreezeShardReply struct {
	State []byte
	Num   model.Tnum
	Err   model.Err
}

type InstallShardArgs struct {
	Shard model.Tshid
	State []byte
	Num   model.Tnum
}

type InstallShardReply struct {
	Err model.Err
}

type DeleteShardArgs struct {
	Shard model.Tshid
	Num   model.Tnum
}

type DeleteShardReply struct {
	Err model.Err
}
