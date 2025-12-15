package config

import (
	"encoding/json"
	"log"
	"runtime/debug"
	"slices"
	"testing"

	"github.com/khyallin/shardkv/model"
)

const (
	Port         = ":8000"
	Maxraftstate = 1024 * 1024 * 10 // 10MB
)

const (
	NumFirst = model.Tnum(1)
	Gid0     = model.Tgid(0)
	Gid1     = model.Tgid(1)
)

// A configuration -- an assignment of shards to groups.
type Config struct {
	Num    model.Tnum                // config number
	Shards [model.NShards]model.Tgid // shard -> gid
	Groups map[model.Tgid][]string   // gid -> servers[]
}

func MakeConfig() *Config {
	c := &Config{
		Groups: make(map[model.Tgid][]string),
	}
	return c
}

func DefaultConfig() *Config {
	return defaultConfig.Copy()
}

func (cfg *Config) String() string {
	b, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		log.Fatalf("Marshal err %v", err)
	}
	return string(b)
}

func FromString(s string) *Config {
	scfg := &Config{}
	if err := json.Unmarshal([]byte(s), scfg); err != nil {
		log.Fatalf("Unmarshal err %v", err)
	}
	return scfg
}

func (cfg *Config) Copy() *Config {
	c := MakeConfig()
	c.Num = cfg.Num
	c.Shards = cfg.Shards
	for k, srvs := range cfg.Groups {
		s := make([]string, len(srvs))
		copy(s, srvs)
		c.Groups[k] = s
	}
	return c
}

// mostgroup, mostn, leastgroup, leastn
func analyze(c *Config) (model.Tgid, int, model.Tgid, int) {
	counts := map[model.Tgid]int{}
	for _, g := range c.Shards {
		counts[g] += 1
	}

	mn := -1
	var mg model.Tgid = -1
	ln := 257
	var lg model.Tgid = -1
	// Enforce deterministic ordering, map iteration
	// is randomized in go
	groups := make([]model.Tgid, len(c.Groups))
	i := 0
	for k := range c.Groups {
		groups[i] = k
		i++
	}
	slices.Sort(groups)
	for _, g := range groups {
		if counts[g] < ln {
			ln = counts[g]
			lg = g
		}
		if counts[g] > mn {
			mn = counts[g]
			mg = g
		}
	}

	return mg, mn, lg, ln
}

// return GID of group with least number of
// assigned shards.
func least(c *Config) model.Tgid {
	_, _, lg, _ := analyze(c)
	return lg
}

// balance assignment of shards to groups.
// modifies c.
func (c *Config) Rebalance() {
	// if no groups, un-assign all shards
	if len(c.Groups) < 1 {
		for s, _ := range c.Shards {
			c.Shards[s] = 0
		}
		return
	}

	// assign all unassigned shards
	for s, g := range c.Shards {
		_, ok := c.Groups[g]
		if ok == false {
			lg := least(c)
			c.Shards[s] = lg
		}
	}

	// move shards from most to least heavily loaded
	for {
		mg, mn, lg, ln := analyze(c)
		if mn < ln+2 {
			break
		}
		// move 1 shard from mg to lg
		for s, g := range c.Shards {
			if g == mg {
				c.Shards[s] = lg
				break
			}
		}
	}
}

func (cfg *Config) Join(servers map[model.Tgid][]string) bool {
	changed := false
	for gid, servers := range servers {
		_, ok := cfg.Groups[gid]
		if ok {
			log.Printf("re-Join %v", gid)
			return false
		}
		for xgid, xservers := range cfg.Groups {
			for _, s1 := range xservers {
				for _, s2 := range servers {
					if s1 == s2 {
						log.Fatalf("Join(%v) puts server %v in groups %v and %v", gid, s1, xgid, gid)
					}
				}
			}
		}
		// new GID
		// modify cfg to reflect the Join()
		cfg.Groups[gid] = servers
		changed = true
	}
	if changed == false {
		log.Fatalf("Join but no change")
	}
	cfg.Num += 1
	return true
}

func (cfg *Config) Leave(gids []model.Tgid) bool {
	changed := false
	for _, gid := range gids {
		_, ok := cfg.Groups[gid]
		if ok == false {
			// already no GID!
			log.Printf("Leave(%v) but not in config", gid)
			return false
		} else {
			// modify op.Config to reflect the Leave()
			delete(cfg.Groups, gid)
			changed = true
		}
	}
	if changed == false {
		debug.PrintStack()
		log.Fatalf("Leave but no change")
	}
	cfg.Num += 1
	return true
}

func (cfg *Config) JoinBalance(servers map[model.Tgid][]string) bool {
	if !cfg.Join(servers) {
		return false
	}
	cfg.Rebalance()
	return true
}

func (cfg *Config) LeaveBalance(gids []model.Tgid) bool {
	if !cfg.Leave(gids) {
		return false
	}
	cfg.Rebalance()
	return true
}

func (cfg *Config) GidServers(sh model.Tshid) (model.Tgid, []string, bool) {
	gid := cfg.Shards[sh]
	srvs, ok := cfg.Groups[gid]
	return gid, srvs, ok
}

func (cfg *Config) IsMember(gid model.Tgid) bool {
	for _, g := range cfg.Shards {
		if g == gid {
			return true
		}
	}
	return false
}

func (cfg *Config) CheckConfig(t *testing.T, groups []model.Tgid) {
	if len(cfg.Groups) != len(groups) {
		fatalf(t, "wanted %v groups, got %v", len(groups), len(cfg.Groups))
	}

	// are the groups as expected?
	for _, g := range groups {
		_, ok := cfg.Groups[g]
		if ok != true {
			fatalf(t, "missing group %v", g)
		}
	}

	// any un-allocated shards?
	if len(groups) > 0 {
		for s, g := range cfg.Shards {
			_, ok := cfg.Groups[g]
			if ok == false {
				fatalf(t, "shard %v -> invalid group %v", s, g)
			}
		}
	}

	// more or less balanced sharding?
	counts := map[model.Tgid]int{}
	for _, g := range cfg.Shards {
		counts[g] += 1
	}
	min := 257
	max := 0
	for g, _ := range cfg.Groups {
		if counts[g] > max {
			max = counts[g]
		}
		if counts[g] < min {
			min = counts[g]
		}
	}
	if max > min+1 {
		fatalf(t, "max %v too much larger than min %v", max, min)
	}
}

func fatalf(t *testing.T, format string, args ...any) {
	debug.PrintStack()
	t.Fatalf(format, args...)
}
