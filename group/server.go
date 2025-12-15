package group

import (
	"sync/atomic"

	"github.com/khyallin/shardkv/model"
	"github.com/khyallin/shardkv/raft"
	"github.com/khyallin/shardkv/rpc"
	"github.com/khyallin/shardkv/rsm"
	"github.com/khyallin/shardkv/statemachine"
)

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) error {
	err, result := kv.rsm.Submit(args)
	if err != model.OK {
		reply.Err = err
		return nil
	}
	rep := result.(*rpc.GetReply)
	reply.Value = rep.Value
	reply.Version = rep.Version
	reply.Err = rep.Err
	return nil
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) error {
	err, result := kv.rsm.Submit(args)
	if err != model.OK {
		reply.Err = err
		return nil
	}
	reply.Err = result.(*rpc.PutReply).Err
	return nil
}

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *rpc.FreezeShardArgs, reply *rpc.FreezeShardReply) error {
	err, result := kv.rsm.Submit(args)
	if err != model.OK {
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
	if err != model.OK {
		reply.Err = err
		return nil
	}
	reply.Err = result.(*rpc.InstallShardReply).Err
	return nil
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *rpc.DeleteShardArgs, reply *rpc.DeleteShardReply) error {
	err, result := kv.rsm.Submit(args)
	if err != model.OK {
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

func MakeKVServer(servers []string, gid model.Tgid, me int, persister *raft.Persister, maxraftstate int) (*KVServer, raft.Raft) {
	sm := statemachine.NewMemoryKV(gid, me)
	kv := &KVServer{
		me:  me,
		rsm: rsm.MakeRSM(servers, me, persister, maxraftstate, sm),
	}
	return kv, kv.rsm.Raft()
}
