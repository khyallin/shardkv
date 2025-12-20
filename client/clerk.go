package client

//
// client code to talk to a sharded key/value service.
//
// the client uses the shardctrler to query for the current
// configuration and find the assignment of shards (keys) to groups,
// and then talks to the group that holds the key's shard.
//

import (
	"log"
	"time"

	"github.com/khyallin/shardkv/api"
	"github.com/khyallin/shardkv/config"
	"github.com/khyallin/shardkv/controller"
	"github.com/khyallin/shardkv/internal/group"
	"github.com/khyallin/shardkv/internal/rpc"
)

type Clerk struct {
	cfg       *config.Config
	ctrler    *controller.Controller
	grpClerks map[config.Tgid]*group.Clerk
}

func MakeClerk(servers []string) *Clerk {
	ck := &Clerk{
		cfg:       nil,
		ctrler:    controller.MakeController(servers),
		grpClerks: make(map[config.Tgid]*group.Clerk),
	}
	return ck
}

func (ck *Clerk) grpClerk(key string) *group.Clerk {
	gid, srvs, _ := ck.cfg.GidServers(config.Key2Shard(key))
	grpClerk, ok := ck.grpClerks[gid]
	if !ok {
		grpClerk = group.MakeClerk(gid, srvs)
		ck.grpClerks[gid] = grpClerk
	}
	return grpClerk
}

func (ck *Clerk) Get(key string) (string, api.Tversion, api.Err) {
	for {
		if ck.cfg == nil {
			log.Printf("KVClerk.Get()|UpdateConfig|key=%s|shard=%d", key, config.Key2Shard(key))
			ck.cfg = ck.ctrler.Query()
			ck.grpClerks = make(map[config.Tgid]*group.Clerk)
		}

		value, version, err := ck.grpClerk(key).Get(key)
		if err == rpc.ErrWrongGroup {
			ck.cfg = nil
			time.Sleep(time.Millisecond * 100)
			continue
		}
		return value, version, err
	}
}

func (ck *Clerk) Put(key string, value string, version api.Tversion) api.Err {
	var retry group.Retry
	for {
		if ck.cfg == nil {
			log.Printf("KVClerk.Put()|UpdateConfig|key=%s|shard=%d|value=%v|version=%d", key, config.Key2Shard(key), value, version)
			ck.cfg = ck.ctrler.Query()
			ck.grpClerks = make(map[config.Tgid]*group.Clerk)
		}

		err := ck.grpClerk(key).Put(key, value, version, &retry)
		if err == rpc.ErrWrongGroup {
			ck.cfg = nil
			retry.Set()
			time.Sleep(time.Millisecond * 100)
			continue
		}
		if err == api.ErrVersion && retry.Get() {
			err = api.ErrMaybe
		}
		return err
	}
}
