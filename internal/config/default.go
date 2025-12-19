package config

const (
	Port         = ":8000"
	Maxraftstate = 1024 * 1024 * 10 // 10MB
	NShards      = 12
)

const (
	NumFirst = Tnum(1)
	Gid0     = Tgid(0)
	Gid1     = Tgid(1)
)

var defaultConfig = &Config{
	Num:    1,
	Shards: [NShards]Tgid{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
	Groups: map[Tgid][]string{
		0: {"172.20.0.10", "172.20.0.11", "172.20.0.12"},
		1: {"172.20.0.13", "172.20.0.14", "172.20.0.15"},
	},
}
