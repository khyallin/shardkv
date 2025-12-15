package rpc

import "github.com/khyallin/shardkv/model"

type PutArgs struct {
	Key     string
	Value   string
	Version model.Tversion
}

type PutReply struct {
	Err model.Err
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Value   string
	Version model.Tversion
	Err     model.Err
}
