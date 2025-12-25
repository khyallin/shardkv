package config

const (
	Port         = ":8380"
	AdminPort    = ":8379"
	Maxraftstate = 1024 * 1024 * 10 // 10MB
	NShards      = 12
)

const (
	NumFirst = Tnum(1)
	Gid0     = Tgid(0)
	Gid1     = Tgid(1)
)
