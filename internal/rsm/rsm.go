package rsm

import (
	"encoding/gob"
	"sync"
	"time"

	"github.com/khyallin/shardkv/api"
	"github.com/khyallin/shardkv/internal/persist"
	"github.com/khyallin/shardkv/internal/raft"
	"github.com/khyallin/shardkv/internal/rpc"
)

type Op struct {
	Req any
	Me  int
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raft.Raft
	applyCh      chan raft.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine

	notifyCh map[int]chan any
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []string, me int, maxraftstate int, sm StateMachine) *RSM {
	gob.Register(Op{})

	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raft.ApplyMsg),
		sm:           sm,

		notifyCh: make(map[int]chan any),
	}

	snapshot := persist.NewBlock("snapshot")
	if snapshot.Size() > 0 {
		rsm.sm.Restore(snapshot.Read())
	}

	rsm.rf = raft.Make(servers, me, snapshot, rsm.applyCh)
	go rsm.reader()

	return rsm
}

func (rsm *RSM) Raft() raft.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (api.Err, any) {
	rsm.mu.Lock()
	op := Op{
		Req: req,
		Me:  rsm.me,
	}
	index, _, isLeader := rsm.rf.Start(op)
	if !isLeader {
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}

	ch := make(chan any, 1)
	rsm.notifyCh[index] = ch
	rsm.mu.Unlock()

	select {
	case reply := <-ch:
		if reply == nil {
			return rpc.ErrWrongLeader, nil
		} else {
			return api.OK, reply
		}
	case <-time.After(time.Second * 2):
		rsm.mu.Lock()
		delete(rsm.notifyCh, index)
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}
}

func (rsm *RSM) reader() {
	for msg := range rsm.applyCh {
		if msg.CommandValid {
			rsm.handleCommand(msg.Command.(Op), msg.CommandIndex)
		}
		if msg.SnapshotValid {
			rsm.sm.Restore(msg.Snapshot)
		}

		if rsm.maxraftstate != -1 && rsm.rf.PersistBytes() > rsm.maxraftstate {
			snapshot := rsm.sm.Snapshot()
			rsm.rf.Snapshot(msg.CommandIndex, snapshot)
		}
	}

	rsm.mu.Lock()
	for index, ch := range rsm.notifyCh {
		ch <- nil
		delete(rsm.notifyCh, index)
	}
	rsm.mu.Unlock()
}

func (rsm *RSM) handleCommand(op Op, index int) {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()

	resp := rsm.sm.DoOp(op.Req)
	ch, ok := rsm.notifyCh[index]
	if !ok {
		return
	}

	if op.Me != rsm.me {
		ch <- nil
	} else {
		ch <- resp
	}
	delete(rsm.notifyCh, index)
}
