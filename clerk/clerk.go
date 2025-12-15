package clerk

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

	"github.com/khyallin/shardkv/config"
	"github.com/khyallin/shardkv/controller"
	"github.com/khyallin/shardkv/group"
	"github.com/khyallin/shardkv/model"
	"github.com/khyallin/shardkv/util"
)

type Clerk struct {
	cfg       *config.Config
	ctrler    *controller.Controller
	grpClerks map[model.Tgid]*group.Clerk
}

func MakeClerk() *Clerk {
	ck := &Clerk{
		cfg:       nil,
		ctrler:    controller.MakeController(),
		grpClerks: make(map[model.Tgid]*group.Clerk),
	}
	return ck
}

func (ck *Clerk) grpClerk(key string) *group.Clerk {
	gid, srvs, _ := ck.cfg.GidServers(util.Key2Shard(key))
	grpClerk, ok := ck.grpClerks[gid]
	if !ok {
		grpClerk = group.MakeClerk(gid, srvs)
		ck.grpClerks[gid] = grpClerk
	}
	return grpClerk
}

func (ck *Clerk) Get(key string) (string, model.Tversion, model.Err) {
	for {
		if ck.cfg == nil {
			log.Printf("KVClerk.Get()|UpdateConfig|key=%s|shard=%d", key, util.Key2Shard(key))
			ck.cfg = ck.ctrler.Query()
			ck.grpClerks = make(map[model.Tgid]*group.Clerk)
		}

		value, version, err := ck.grpClerk(key).Get(key)
		if err == model.ErrWrongGroup {
			ck.cfg = nil
			time.Sleep(time.Millisecond * 100)
			continue
		}
		return value, version, err
	}
}

func (ck *Clerk) Put(key string, value string, version model.Tversion) model.Err {
	var retry group.Retry
	for {
		if ck.cfg == nil {
			log.Printf("KVClerk.Put()|UpdateConfig|key=%s|shard=%d|value=%v|version=%d", key, util.Key2Shard(key), value, version)
			ck.cfg = ck.ctrler.Query()
			ck.grpClerks = make(map[model.Tgid]*group.Clerk)
		}

		err := ck.grpClerk(key).Put(key, value, version, &retry)
		if err == model.ErrWrongGroup {
			ck.cfg = nil
			retry.Set()
			time.Sleep(time.Millisecond * 100)
			continue
		}
		if err == model.ErrVersion && retry.Get() {
			err = model.ErrMaybe
		}
		return err
	}
}
