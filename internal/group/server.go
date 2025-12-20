package group

import (
	"encoding/gob"
	"sync/atomic"

	"github.com/khyallin/shardkv/api"
	"github.com/khyallin/shardkv/config"
	"github.com/khyallin/shardkv/internal/rpc"
	"github.com/khyallin/shardkv/internal/rsm"
	"github.com/khyallin/shardkv/internal/statemachine"
)

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM
}

func (kv *KVServer) Get(args *api.GetArgs, reply *api.GetReply) error {
	err, result := kv.rsm.Submit(args)
	if err != api.OK {
		reply.Err = err
		return nil
	}
	rep := result.(*api.GetReply)
	reply.Value = rep.Value
	reply.Version = rep.Version
	reply.Err = rep.Err
	return nil
}

func (kv *KVServer) Put(args *api.PutArgs, reply *api.PutReply) error {
	err, result := kv.rsm.Submit(args)
	if err != api.OK {
		reply.Err = err
		return nil
	}
	reply.Err = result.(*api.PutReply).Err
	return nil
}

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *rpc.FreezeShardArgs, reply *rpc.FreezeShardReply) error {
	err, result := kv.rsm.Submit(args)
	if err != api.OK {
		reply.Err = err
		return nil
	}
	rep := result.(*rpc.FreezeShardReply)
	reply.State = rep.State
	reply.Num = rep.Num
	reply.Err = rep.Err
	return nil
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *rpc.InstallShardArgs, reply *rpc.InstallShardReply) error {
	err, result := kv.rsm.Submit(args)
	if err != api.OK {
		reply.Err = err
		return nil
	}
	reply.Err = result.(*rpc.InstallShardReply).Err
	return nil
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *rpc.DeleteShardArgs, reply *rpc.DeleteShardReply) error {
	err, result := kv.rsm.Submit(args)
	if err != api.OK {
		reply.Err = err
		return nil
	}
	reply.Err = result.(*rpc.DeleteShardReply).Err
	return nil
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func MakeKVServer(servers []string, gid config.Tgid, me int, maxraftstate int) []Service {
	gob.Register(&api.GetArgs{})
	gob.Register(&api.GetReply{})
	gob.Register(&api.PutArgs{})
	gob.Register(&api.PutReply{})
	gob.Register(&rpc.FreezeShardArgs{})
	gob.Register(&rpc.FreezeShardReply{})
	gob.Register(&rpc.InstallShardArgs{})
	gob.Register(&rpc.InstallShardReply{})
	gob.Register(&rpc.DeleteShardArgs{})
	gob.Register(&rpc.DeleteShardReply{})

	sm := statemachine.NewMemoryKV(gid, me)
	kv := &KVServer{
		me:  me,
		rsm: rsm.MakeRSM(servers, me, maxraftstate, sm),
	}
	return []Service{kv, kv.rsm.Raft()}
}
