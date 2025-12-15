package config

import "github.com/khyallin/shardkv/model"

var defaultConfig = &Config{
	Num:    1,
	Shards: [model.NShards]model.Tgid{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
	Groups: map[model.Tgid][]string{
		0: {"172.20.0.10", "172.20.0.11", "172.20.0.12"},
		1: {"172.20.0.13", "172.20.0.14", "172.20.0.15"},
	},
}
