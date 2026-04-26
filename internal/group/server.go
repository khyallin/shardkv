package group

import (
	"encoding/gob"
	"sync/atomic"

	"github.com/khyallin/shardkv/api"
	"github.com/khyallin/shardkv/config"
	"github.com/khyallin/shardkv/internal/raft"
	"github.com/khyallin/shardkv/internal/rpc"
	"github.com/khyallin/shardkv/internal/rsm"
	"github.com/khyallin/shardkv/internal/statemachine"
)

type KVServer struct {
	me      int
	dead    int32 // set by Kill()
	rsm     *rsm.RSM
	metrics *Metrics
}

func (kv *KVServer) Get(args *api.GetArgs, reply *api.GetReply) error {
	start := kv.metrics.OnIngress()
	defer func() {
		kv.metrics.OnDone(start, reply.Err)
	}()

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
	start := kv.metrics.OnIngress()
	defer func() {
		kv.metrics.OnDone(start, reply.Err)
	}()

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
	start := kv.metrics.OnIngress()
	defer func() {
		kv.metrics.OnDone(start, reply.Err)
	}()

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
	start := kv.metrics.OnIngress()
	defer func() {
		kv.metrics.OnDone(start, reply.Err)
	}()

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
	start := kv.metrics.OnIngress()
	defer func() {
		kv.metrics.OnDone(start, reply.Err)
	}()

	err, result := kv.rsm.Submit(args)
	if err != api.OK {
		reply.Err = err
		return nil
	}
	reply.Err = result.(*rpc.DeleteShardReply).Err
	return nil
}

func (kv *KVServer) Status(args *api.StatusArgs, reply *api.StatusReply) error {
	view := kv.metrics.View()
	reply.TotalQPS = view.TotalQPS(Time)
	reply.DoneQPS = view.DoneQPS(Time)
	reply.SuccessQPS = view.SuccessQPS(Time)
	reply.MaxLatency = view.MaxLatency(Time)
	reply.AvgLatency = view.AvgLatency(Time)
	if _, isLeader := kv.rsm.Raft().GetState(); isLeader {
		reply.Err = api.OK
	} else {
		reply.Err = rpc.ErrWrongLeader
	}
	return nil
}

// Kill() is called when a KVServer instance won't be needed again.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func MakeKVServer(servers []string, gid config.Tgid, me int, maxraftstate int) (*KVServer, *raft.Raft) {
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
		me:      me,
		rsm:     rsm.MakeRSM(servers, me, maxraftstate, sm),
		metrics: MakeMetrics(),
	}
	return kv, kv.rsm.Raft()
}
