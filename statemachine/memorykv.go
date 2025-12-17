package statemachine

import (
	"bytes"
	"encoding/gob"
	"log"

	"github.com/khyallin/shardkv/config"
	"github.com/khyallin/shardkv/model"
	"github.com/khyallin/shardkv/rpc"
	"github.com/khyallin/shardkv/util"
)

type Tversion int

type Record struct {
	Version model.Tversion
	Value   string
}

type MemoryKV struct {
	gid       model.Tgid
	me        int
	data      []map[string]Record
	shardNums []model.Tnum
	shardExts []bool
}

func NewMemoryKV(gid model.Tgid, me int) *MemoryKV {
	kv := &MemoryKV{
		gid:       gid,
		me:        me,
		data:      make([]map[string]Record, model.NShards),
		shardNums: make([]model.Tnum, model.NShards),
		shardExts: make([]bool, model.NShards),
	}
	cfg := config.DefaultConfig()
	for i := 0; i < model.NShards; i++ {
		kv.data[i] = make(map[string]Record)
		kv.shardNums[i] = config.NumFirst
		if gid == config.Gid0 || gid == cfg.Shards[i] {
			kv.shardExts[i] = true
		}
	}
	return kv
}

func (kv *MemoryKV) DoOp(req any) any {
	switch req.(type) {
	case *rpc.GetArgs, rpc.GetArgs:
		return kv.doGet(req)
	case *rpc.PutArgs, rpc.PutArgs:
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
	var shardNums []model.Tnum
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
	var args *rpc.GetArgs
	if p, ok := req.(*rpc.GetArgs); ok {
		args = p
	} else {
		val := req.(rpc.GetArgs)
		args = &val
	}
	value, version, err := kv.get(args.Key)
	return &rpc.GetReply{
		Value:   value,
		Version: version,
		Err:     err,
	}
}

func (kv *MemoryKV) doPut(req any) any {
	var args *rpc.PutArgs
	if p, ok := req.(*rpc.PutArgs); ok {
		args = p
	} else {
		val := req.(rpc.PutArgs)
		args = &val
	}
	err := kv.put(args.Key, args.Value, args.Version)
	return &rpc.PutReply{Err: err}
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

func (kv *MemoryKV) get(key string) (string, model.Tversion, model.Err) {
	shard := util.Key2Shard(key)
	if !kv.shardExts[shard] {
		log.Printf("MemoryKV%d-%d.Get()|ErrWrongGroup|key=%s|shard=%d", kv.gid, kv.me, key, shard)
		return "", 0, model.ErrWrongGroup
	}
	data := kv.data[shard]

	record, ok := data[key]
	if !ok {
		log.Printf("MemoryKV%d-%d.Get()|ErrNoKey|key=%s|shard=%d", kv.gid, kv.me, key, shard)
		return "", 0, model.ErrNoKey
	}

	log.Printf("MemoryKV%d-%d.Get()|OK|key=%s|shard=%d|value=%s|version=%d", kv.gid, kv.me, key, shard, record.Value, record.Version)
	return record.Value, record.Version, model.OK
}

func (kv *MemoryKV) put(key string, value string, version model.Tversion) model.Err {
	shard := util.Key2Shard(key)
	if !kv.shardExts[shard] {
		log.Printf("MemoryKV%d-%d.Put()|ErrWrongGroup|key=%s|shard=%d|value=%v|version=%d", kv.gid, kv.me, key, shard, value, version)
		return model.ErrWrongGroup
	}
	data := kv.data[shard]

	record, ok := data[key]
	if !ok && version != 0 {
		log.Printf("MemoryKV%d-%d.Put()|ErrNoKey|key=%s|shard=%d|value=%v|version=%d", kv.gid, kv.me, key, shard, value, version)
		return model.ErrNoKey
	}
	if ok && version != record.Version {
		log.Printf("MemoryKV%d-%d.Put()|ErrVersion|key=%s|shard=%d|value=%v|want_version=%d|given_version=%d", kv.gid, kv.me, key, shard, record.Value, record.Version, version)
		return model.ErrVersion
	}

	record.Value = value
	record.Version++
	data[key] = record

	log.Printf("MemoryKV%d-%d.Put()|OK|key=%s|shard=%d|value=%v|version=%d", kv.gid, kv.me, key, shard, value, version)
	return model.OK
}

func (kv *MemoryKV) freezeShard(shard model.Tshid, num model.Tnum) ([]byte, model.Tnum, model.Err) {
	if num < kv.shardNums[shard] {
		log.Printf("MemoryKV%d-%d.FreezeShard()|ErrWrongGroup|shard=%d|given_num=%d|current_num=%d", kv.gid, kv.me, shard, num, kv.shardNums[shard])
		return nil, kv.shardNums[shard], model.ErrWrongGroup
	}
	kv.shardNums[shard] = num

	kv.shardExts[shard] = false
	b := new(bytes.Buffer)
	e := gob.NewEncoder(b)
	e.Encode(kv.data[shard])
	log.Printf("MemoryKV%d-%d.FreezeShard()|OK|shard=%d|num=%d", kv.gid, kv.me, shard, num)
	return b.Bytes(), num, model.OK
}

func (kv *MemoryKV) installShard(shard model.Tshid, state []byte, num model.Tnum) model.Err {
	if num < kv.shardNums[shard] {
		log.Printf("MemoryKV%d-%d.InstallShard()|ErrWrongGroup|shard=%d|given_num=%d|current_num=%d", kv.gid, kv.me, shard, num, kv.shardNums[shard])
		return model.ErrWrongGroup
	}
	if num == kv.shardNums[shard] {
		log.Printf("MemoryKV%d-%d.InstallShard()|Repeat|shard=%d|num=%d", kv.gid, kv.me, shard, num)
		return model.OK
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
	return model.OK
}

func (kv *MemoryKV) deleteShard(shard model.Tshid, num model.Tnum) model.Err {
	if num < kv.shardNums[shard] {
		log.Printf("MemoryKV%d-%d.DeleteShard()|ErrWrongGroup|shard=%d|given_num=%d|current_num=%d", kv.gid, kv.me, shard, num, kv.shardNums[shard])
		return model.ErrWrongGroup
	}
	kv.shardNums[shard] = num

	kv.data[shard] = make(map[string]Record)
	log.Printf("MemoryKV%d-%d.DeleteShard()|OK|shard=%d|num=%d", kv.gid, kv.me, shard, num)
	return model.OK
}
