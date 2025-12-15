package group

import (
	"log"
	"math/rand"
	"time"

	"github.com/khyallin/shardkv/model"
	"github.com/khyallin/shardkv/rpc"
	"github.com/khyallin/shardkv/util"
)

const (
	Interval = time.Millisecond * 100
)

type Clerk struct {
	clients   []*rpc.Client
	gid       model.Tgid
	prvLeader int
}

func MakeClerk(gid model.Tgid, servers []string) *Clerk {
	ck := &Clerk{
		clients:   make([]*rpc.Client, len(servers)),
		gid:       gid,
		prvLeader: -1,
	}
	for i, server := range servers {
		ck.clients[i] = rpc.NewClient(server)
	}
	return ck
}

func (ck *Clerk) peer() int {
	if ck.prvLeader >= 0 {
		return ck.prvLeader
	}
	return rand.Intn(len(ck.clients))
}

func (ck *Clerk) Get(key string) (string, model.Tversion, model.Err) {
	log.Printf("GroupClerk%d.Get()|Start|key=%s|shard=%d", ck.gid, key, util.Key2Shard(key))

	args := &rpc.GetArgs{Key: key}
	ddl := time.Now().Add(2 * time.Second)

	for time.Now().Before(ddl) {
		reply := &rpc.GetReply{}
		peer := ck.peer()
		ok := ck.clients[peer].Call("KVServer.Get", args, reply)
		if !ok || reply.Err == model.ErrWrongLeader {
			ck.prvLeader = -1
			time.Sleep(Interval)
			continue
		}
		ck.prvLeader = peer
		log.Printf("GroupClerk%d.Get()|End|key=%s|shard=%d|value=%s|version=%d|err=%v", ck.gid, key, util.Key2Shard(key), reply.Value, reply.Version, reply.Err)
		return reply.Value, reply.Version, reply.Err
	}
	return "", 0, model.ErrWrongGroup
}

func (ck *Clerk) Put(key string, value string, version model.Tversion, retry *Retry) model.Err {
	log.Printf("GroupClerk%d.Put()|Start|key=%s|shard=%d|value=%s|version=%d", ck.gid, key, util.Key2Shard(key), value, version)
	args := &rpc.PutArgs{
		Key:     key,
		Value:   value,
		Version: version,
	}
	ddl := time.Now().Add(4 * time.Second)

	for time.Now().Before(ddl) {
		reply := &rpc.PutReply{}
		peer := ck.peer()
		ok := ck.clients[peer].Call("KVServer.Put", args, reply)
		if !ok || reply.Err == model.ErrWrongLeader {
			ck.prvLeader = -1
			retry.Set()
			time.Sleep(Interval)
			continue
		}
		ck.prvLeader = peer
		log.Printf("GroupClerk%d.Put()|End|key=%s|shard=%d|value=%v|version=%d|err=%v", ck.gid, key, util.Key2Shard(key), value, version, reply.Err)
		return reply.Err
	}
	return model.ErrWrongGroup
}

func (ck *Clerk) FreezeShard(s model.Tshid, num model.Tnum) ([]byte, model.Tnum, model.Err) {
	args := &rpc.FreezeShardArgs{
		Shard: s,
		Num:   num,
	}
	ddl := time.Now().Add(4 * time.Second)

	log.Printf("GroupClerk%d.FreezeShard()|Start|shard=%d|num=%d", ck.gid, s, num)
	for time.Now().Before(ddl) {
		reply := &rpc.FreezeShardReply{}
		peer := ck.peer()
		ok := ck.clients[peer].Call("KVServer.FreezeShard", args, reply)
		if !ok || reply.Err == model.ErrWrongLeader {
			ck.prvLeader = -1
			time.Sleep(Interval)
			continue
		}
		ck.prvLeader = peer
		log.Printf("GroupClerk%d.FreezeShard()|End|shard=%d|num=%d|err=%v", ck.gid, s, num, reply.Err)
		return reply.State, reply.Num, reply.Err
	}
	return nil, 0, model.ErrWrongGroup
}

func (ck *Clerk) InstallShard(s model.Tshid, state []byte, num model.Tnum) model.Err {
	args := &rpc.InstallShardArgs{
		Shard: s,
		State: state,
		Num:   num,
	}
	ddl := time.Now().Add(4 * time.Second)

	log.Printf("GroupClerk%d.InstallShard()|Start|shard=%d|state=%v|num=%d", ck.gid, s, state, num)
	for time.Now().Before(ddl) {
		reply := &rpc.InstallShardReply{}
		peer := ck.peer()
		ok := ck.clients[peer].Call("KVServer.InstallShard", args, reply)
		if !ok || reply.Err == model.ErrWrongLeader {
			ck.prvLeader = -1
			time.Sleep(Interval)
			continue
		}
		ck.prvLeader = peer
		log.Printf("GroupClerk%d.InstallShard()|End|shard=%d|num=%d|err=%v", ck.gid, s, num, reply.Err)
		return reply.Err
	}
	return model.ErrWrongGroup
}

func (ck *Clerk) DeleteShard(s model.Tshid, num model.Tnum) model.Err {
	args := &rpc.DeleteShardArgs{
		Shard: s,
		Num:   num,
	}
	ddl := time.Now().Add(4 * time.Second)

	log.Printf("GroupClerk%d.DeleteShard()|Start|shard=%d|num=%d", ck.gid, s, num)
	for time.Now().Before(ddl) {
		reply := &rpc.DeleteShardReply{}
		peer := ck.peer()
		ok := ck.clients[peer].Call("KVServer.DeleteShard", args, reply)
		if !ok || reply.Err == model.ErrWrongLeader {
			ck.prvLeader = -1
			time.Sleep(Interval)
			continue
		}
		ck.prvLeader = peer
		log.Printf("GroupClerk%d.DeleteShard()|End|shard=%d|num=%d|err=%v", ck.gid, s, num, reply.Err)
		return reply.Err
	}
	return model.ErrWrongGroup
}

type Retry struct {
	r bool
}

func (r *Retry) Set() {
	r.r = true
}

func (r *Retry) Get() bool {
	return r.r
}
