package statemachine

import (
	"bytes"
	"encoding/gob"
	"log"

	"github.com/khyallin/shardkv/api"
	"github.com/khyallin/shardkv/config"
	"github.com/khyallin/shardkv/internal/rpc"
)

type Tversion int

type Record struct {
	Version api.Tversion
	Value   string
}

type MemoryKV struct {
	gid       config.Tgid
	me        int
	data      []map[string]Record
	shardNums []config.Tnum
	shardExts []bool
}

func NewMemoryKV(gid config.Tgid, me int) *MemoryKV {
	kv := &MemoryKV{
		gid:       gid,
		me:        me,
		data:      make([]map[string]Record, config.NShards),
		shardNums: make([]config.Tnum, config.NShards),
		shardExts: make([]bool, config.NShards),
	}
	for i := 0; i < config.NShards; i++ {
		kv.data[i] = make(map[string]Record)
		kv.shardNums[i] = config.NumFirst
		if gid == config.Gid0 {
			kv.shardExts[i] = true
		}
	}
	return kv
}

func (kv *MemoryKV) DoOp(req any) any {
	switch req.(type) {
	case *api.GetArgs, api.GetArgs:
		return kv.doGet(req)
	case *api.PutArgs, api.PutArgs:
		return kv.doPut(req)
	case *rpc.FreezeShardArgs, rpc.FreezeShardArgs:
		return kv.doFreezeShard(req)
	case *rpc.InstallShardArgs, rpc.InstallShardArgs:
		return kv.doInstallShard(req)
	case *rpc.DeleteShardArgs, rpc.DeleteShardArgs:
		return kv.doDeleteShard(req)
	}
	log.Fatalf("MemoryKV%d-%d.DoOp()|UnknownOp|req=%v", kv.gid, kv.me, req)
	return nil
}

func (kv *MemoryKV) Snapshot() []byte {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(kv.data)
	e.Encode(kv.shardNums)
	e.Encode(kv.shardExts)
	log.Printf("MemoryKV%d-%d.Snapshot()|OK", kv.gid, kv.me)
	return w.Bytes()
}

func (kv *MemoryKV) Restore(buffer []byte) {
	r := bytes.NewBuffer(buffer)
	d := gob.NewDecoder(r)
	var data []map[string]Record
	var shardNums []config.Tnum
	var shardExts []bool
	if d.Decode(&data) != nil ||
		d.Decode(&shardNums) != nil ||
		d.Decode(&shardExts) != nil {
		log.Fatalf("MemoryKV%d-%d.Restore()|Fail", kv.gid, kv.me)
		return
	}

	kv.data = data
	kv.shardNums = shardNums
	kv.shardExts = shardExts
	log.Printf("MemoryKV%d-%d.Restore()|OK", kv.gid, kv.me)
}

func (kv *MemoryKV) doGet(req any) any {
	var args *api.GetArgs
	if p, ok := req.(*api.GetArgs); ok {
		args = p
	} else {
		val := req.(api.GetArgs)
		args = &val
	}
	value, version, err := kv.get(args.Key)
	return &api.GetReply{
		Value:   value,
		Version: version,
		Err:     err,
	}
}

func (kv *MemoryKV) doPut(req any) any {
	var args *api.PutArgs
	if p, ok := req.(*api.PutArgs); ok {
		args = p
	} else {
		val := req.(api.PutArgs)
		args = &val
	}
	err := kv.put(args.Key, args.Value, args.Version)
	return &api.PutReply{Err: err}
}

func (kv *MemoryKV) doFreezeShard(req any) any {
	var args *rpc.FreezeShardArgs
	if p, ok := req.(*rpc.FreezeShardArgs); ok {
		args = p
	} else {
		val := req.(rpc.FreezeShardArgs)
		args = &val
	}
	state, num, err := kv.freezeShard(args.Shard, args.Num)
	return &rpc.FreezeShardReply{
		State: state,
		Num:   num,
		Err:   err,
	}
}

func (kv *MemoryKV) doInstallShard(req any) any {
	var args *rpc.InstallShardArgs
	if p, ok := req.(*rpc.InstallShardArgs); ok {
		args = p
	} else {
		val := req.(rpc.InstallShardArgs)
		args = &val
	}
	err := kv.installShard(args.Shard, args.State, args.Num)
	return &rpc.InstallShardReply{Err: err}
}

func (kv *MemoryKV) doDeleteShard(req any) any {
	var args *rpc.DeleteShardArgs
	if p, ok := req.(*rpc.DeleteShardArgs); ok {
		args = p
	} else {
		val := req.(rpc.DeleteShardArgs)
		args = &val
	}
	err := kv.deleteShard(args.Shard, args.Num)
	return &rpc.DeleteShardReply{Err: err}
}

func (kv *MemoryKV) get(key string) (string, api.Tversion, api.Err) {
	shard := config.Key2Shard(key)
	if !kv.shardExts[shard] {
		log.Printf("MemoryKV%d-%d.Get()|ErrWrongGroup|key=%s|shard=%d", kv.gid, kv.me, key, shard)
		return "", 0, rpc.ErrWrongGroup
	}
	data := kv.data[shard]

	record, ok := data[key]
	if !ok {
		log.Printf("MemoryKV%d-%d.Get()|ErrNoKey|key=%s|shard=%d", kv.gid, kv.me, key, shard)
		return "", 0, api.ErrNoKey
	}

	log.Printf("MemoryKV%d-%d.Get()|OK|key=%s|shard=%d|value=%s|version=%d", kv.gid, kv.me, key, shard, record.Value, record.Version)
	return record.Value, record.Version, api.OK
}

func (kv *MemoryKV) put(key string, value string, version api.Tversion) api.Err {
	shard := config.Key2Shard(key)
	if !kv.shardExts[shard] {
		log.Printf("MemoryKV%d-%d.Put()|ErrWrongGroup|key=%s|shard=%d|value=%v|version=%d", kv.gid, kv.me, key, shard, value, version)
		return rpc.ErrWrongGroup
	}
	data := kv.data[shard]

	record, ok := data[key]
	if !ok && version != 0 {
		log.Printf("MemoryKV%d-%d.Put()|ErrNoKey|key=%s|shard=%d|value=%v|version=%d", kv.gid, kv.me, key, shard, value, version)
		return api.ErrNoKey
	}
	if ok && version != record.Version {
		log.Printf("MemoryKV%d-%d.Put()|ErrVersion|key=%s|shard=%d|value=%v|want_version=%d|given_version=%d", kv.gid, kv.me, key, shard, record.Value, record.Version, version)
		return api.ErrVersion
	}

	record.Value = value
	record.Version++
	data[key] = record

	log.Printf("MemoryKV%d-%d.Put()|OK|key=%s|shard=%d|value=%v|version=%d", kv.gid, kv.me, key, shard, value, version)
	return api.OK
}

func (kv *MemoryKV) freezeShard(shard config.Tshid, num config.Tnum) ([]byte, config.Tnum, api.Err) {
	if num < kv.shardNums[shard] {
		log.Printf("MemoryKV%d-%d.FreezeShard()|ErrWrongGroup|shard=%d|given_num=%d|current_num=%d", kv.gid, kv.me, shard, num, kv.shardNums[shard])
		return nil, kv.shardNums[shard], rpc.ErrWrongGroup
	}
	kv.shardNums[shard] = num

	kv.shardExts[shard] = false
	b := new(bytes.Buffer)
	e := gob.NewEncoder(b)
	e.Encode(kv.data[shard])
	log.Printf("MemoryKV%d-%d.FreezeShard()|OK|shard=%d|num=%d", kv.gid, kv.me, shard, num)
	return b.Bytes(), num, api.OK
}

func (kv *MemoryKV) installShard(shard config.Tshid, state []byte, num config.Tnum) api.Err {
	if num < kv.shardNums[shard] {
		log.Printf("MemoryKV%d-%d.InstallShard()|ErrWrongGroup|shard=%d|given_num=%d|current_num=%d", kv.gid, kv.me, shard, num, kv.shardNums[shard])
		return rpc.ErrWrongGroup
	}
	if num == kv.shardNums[shard] {
		log.Printf("MemoryKV%d-%d.InstallShard()|Repeat|shard=%d|num=%d", kv.gid, kv.me, shard, num)
		return api.OK
	}
	kv.shardNums[shard] = num

	kv.shardExts[shard] = true
	if len(state) == 0 {
		kv.data[shard] = make(map[string]Record)
	} else {
		var shardData map[string]Record
		b := bytes.NewBuffer(state)
		d := gob.NewDecoder(b)
		if d.Decode(&shardData) == nil {
			kv.data[shard] = shardData
		}
	}

	log.Printf("MemoryKV%d-%d.InstallShard()|OK|shard=%d|num=%d", kv.gid, kv.me, shard, num)
	return api.OK
}

func (kv *MemoryKV) deleteShard(shard config.Tshid, num config.Tnum) api.Err {
	if num < kv.shardNums[shard] {
		log.Printf("MemoryKV%d-%d.DeleteShard()|ErrWrongGroup|shard=%d|given_num=%d|current_num=%d", kv.gid, kv.me, shard, num, kv.shardNums[shard])
		return rpc.ErrWrongGroup
	}
	kv.shardNums[shard] = num

	kv.data[shard] = make(map[string]Record)
	log.Printf("MemoryKV%d-%d.DeleteShard()|OK|shard=%d|num=%d", kv.gid, kv.me, shard, num)
	return api.OK
}
