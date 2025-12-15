package util

import (
	"hash/fnv"

	"github.com/khyallin/shardkv/model"
)

func Key2Shard(key string) model.Tshid {
	h := fnv.New32a()
	h.Write([]byte(key))
	shard := model.Tshid(model.Tshid(h.Sum32()) % model.NShards)
	return shard
}
