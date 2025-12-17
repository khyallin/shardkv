package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"encoding/gob"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/khyallin/shardkv/persist"
	"github.com/khyallin/shardkv/rpc"
)

type State int

const (
	Invalid State = iota
	Follower
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type raft struct {
	mu        sync.Mutex    // Lock to protect shared access to this peer's state
	peers     []*rpc.Client // RPC end points of all peers
	me        int           // this peer's index into peers[]
	dead      int32         // set by Kill()
	applyCh   chan ApplyMsg

	state          State
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
	applyCond      *sync.Cond
	replicateCond  []*sync.Cond

	// Persistent state on all servers
	currentTerm *persist.Int
	votedFor    *persist.Int
	log         *Log
	snapshot    *persist.Block

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm.Get(), rf.state == Leader
}

// how many bytes in Raft's persisted log?
func (rf *raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.log.ByteSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.log.Base() || index > rf.log.Len() {
		return
	}
	rf.snapshot.Write(snapshot)
	rf.log.Compact(index)
}

// RequestVote handler

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *raft) makeRequestVoteArgs() *RequestVoteArgs {
	args := &RequestVoteArgs{
		Term:         rf.currentTerm.Get(),
		CandidateId:  rf.me,
		LastLogIndex: rf.log.Last().Index,
		LastLogTerm:  rf.log.Last().Term,
	}
	return args
}

func (rf *raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		return nil
	}

	if args.Term < rf.currentTerm.Get() {
		reply.Term, reply.VoteGranted = rf.currentTerm.Get(), false
		return nil
	}

	if args.Term > rf.currentTerm.Get() {
		rf.currentTerm.Set(args.Term)
		rf.votedFor.Set(-1)
		rf.state = Follower
	}

	if rf.votedFor.Get() != -1 && rf.votedFor.Get() != args.CandidateId {
		reply.Term, reply.VoteGranted = rf.currentTerm.Get(), false
		return nil
	}

	if args.LastLogTerm < rf.log.Last().Term {
		reply.Term, reply.VoteGranted = rf.currentTerm.Get(), false
		return nil
	}
	if args.LastLogTerm == rf.log.Last().Term && args.LastLogIndex < rf.log.Last().Index {
		reply.Term, reply.VoteGranted = rf.currentTerm.Get(), false
		return nil
	}

	rf.votedFor.Set(args.CandidateId)
	reply.Term, reply.VoteGranted = rf.currentTerm.Get(), true
	rf.resetElectionTimer()
	return nil
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *raft) handleRequestVoteReply(peer int, args *RequestVoteArgs, reply *RequestVoteReply, votesReceived *int) {
	if rf.state != Candidate || args.Term != rf.currentTerm.Get() {
		return
	}
	if reply.Term > rf.currentTerm.Get() {
		rf.currentTerm.Set(reply.Term)
		rf.votedFor.Set(-1)
		rf.state = Follower
		return
	}
	if reply.VoteGranted {
		*votesReceived++
		if *votesReceived > len(rf.peers)/2 {
			rf.state = Leader
			for i := range rf.peers {
				rf.nextIndex[i] = rf.log.Len()
				rf.matchIndex[i] = 0
			}
			for i := range rf.peers {
				if i != rf.me {
					rf.replicateCond[i].Signal()
				}
			}
			rf.resetHeartbeatTimer()
		}
	}
}

// AppendEntries handler

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	Xterm   int
	Xindex  int
	Xlen    int
}

func (rf *raft) makeAppendEntriesArgs(peer int) *AppendEntriesArgs {
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm.Get(),
		LeaderId:     rf.me,
		PrevLogIndex: rf.nextIndex[peer] - 1,
		PrevLogTerm:  rf.log.Term(rf.nextIndex[peer] - 1),
		Entries:      rf.log.Tail(rf.nextIndex[peer]),
		LeaderCommit: rf.commitIndex,
	}
	return args
}

func (rf *raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		return nil
	}

	if args.Term < rf.currentTerm.Get() {
		reply.Term, reply.Success = rf.currentTerm.Get(), false
		return nil
	}

	if args.Term > rf.currentTerm.Get() {
		rf.currentTerm.Set(args.Term)
		rf.votedFor.Set(-1)
		rf.state = Follower
	}

	if args.PrevLogIndex >= rf.log.Len() {
		reply.Term, reply.Success = rf.currentTerm.Get(), false
		reply.Xlen = rf.log.Len()
		rf.resetElectionTimer()
		return nil
	}

	if args.PrevLogIndex < rf.log.Base() {
		reply.Term, reply.Success = rf.currentTerm.Get(), false
		reply.Xterm = 0
		reply.Xindex = 0
		reply.Xlen = 1
		rf.resetElectionTimer()
		return nil
	}

	if args.PrevLogTerm != rf.log.Term(args.PrevLogIndex) {
		reply.Term, reply.Success = rf.currentTerm.Get(), false
		reply.Xterm = rf.log.Term(args.PrevLogIndex)
		for i := args.PrevLogIndex; i >= rf.log.Base(); i-- {
			if i == rf.log.Base() || rf.log.Term(i-1) != reply.Xterm {
				reply.Xindex = i
				break
			}
		}
		reply.Xlen = rf.log.Len()
		rf.resetElectionTimer()
		return nil
	}

	rf.resetElectionTimer()
	reply.Term, reply.Success = rf.currentTerm.Get(), true
	rf.state = Follower

	for _, entry := range args.Entries {
		index := entry.Index
		if index < rf.log.Len() {
			if rf.log.Term(index) != entry.Term {
				rf.log.Truncate(index)
				rf.log.Append(entry)
			}
		} else {
			rf.log.Append(entry)
		}
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.log.Last().Index)
		rf.applyCond.Signal()
	}
	return nil
}

func (rf *raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *raft) handleAppendEntriesReply(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.state != Leader || args.Term != rf.currentTerm.Get() {
		return
	}
	if reply.Term > rf.currentTerm.Get() {
		rf.currentTerm.Set(reply.Term)
		rf.votedFor.Set(-1)
		rf.state = Follower
		rf.resetElectionTimer()
		return
	}

	if reply.Success {
		rf.nextIndex[peer] = args.PrevLogIndex + len(args.Entries) + 1
		rf.matchIndex[peer] = rf.nextIndex[peer] - 1
		rf.advanceCommitIndex()
		rf.resetHeartbeatTimer()
		return
	}

	if reply.Xlen < args.PrevLogIndex+1 {
		rf.nextIndex[peer] = reply.Xlen
		rf.resetHeartbeatTimer()
		return
	}

	for i := args.PrevLogIndex; i >= rf.log.Base(); i-- {
		if i == rf.log.Base() || rf.log.Term(i-1) == reply.Xterm {
			rf.nextIndex[peer] = i
			rf.resetHeartbeatTimer()
			return
		}
		if rf.log.Term(i-1) < reply.Xterm {
			break
		}
	}

	rf.nextIndex[peer] = reply.Xindex
	rf.resetHeartbeatTimer()
}

// InstallSnapshot handler

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *raft) makeInstallSnapshotArgs(peer int) *InstallSnapshotArgs {
	args := &InstallSnapshotArgs{
		Term:              rf.currentTerm.Get(),
		LeaderId:          rf.me,
		LastIncludedIndex: rf.log.Base(),
		LastIncludedTerm:  rf.log.Term(rf.log.Base()),
		Data:              rf.snapshot.Read(),
	}
	return args
}

func (rf *raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		return
	}

	if args.Term < rf.currentTerm.Get() {
		reply.Term = rf.currentTerm.Get()
		return
	}

	if args.Term > rf.currentTerm.Get() {
		rf.currentTerm.Set(args.Term)
		rf.votedFor.Set(-1)
		rf.state = Follower
	}

	rf.resetElectionTimer()
	reply.Term = rf.currentTerm.Get()
	if args.LastIncludedIndex <= rf.commitIndex {
		return
	}

	if args.LastIncludedIndex < rf.log.Len() && rf.log.Term(args.LastIncludedIndex) == args.LastIncludedTerm {
		rf.commitIndex = max(rf.commitIndex, args.LastIncludedIndex)
		rf.lastApplied = max(rf.lastApplied, args.LastIncludedIndex)
		rf.log.Compact(args.LastIncludedIndex)
	} else {
		rf.commitIndex = max(rf.commitIndex, args.LastIncludedIndex)
		rf.lastApplied = max(rf.lastApplied, args.LastIncludedIndex)
		rf.log.Clear()
		rf.log.Append(Entry{Index: args.LastIncludedIndex, Term: args.LastIncludedTerm, Command: nil})
	}

	rf.snapshot.Write(args.Data)
	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}

	rf.mu.Unlock()
	go func() {
		defer func() { recover() }()
		if !rf.killed() {
			rf.applyCh <- msg
		}
	}()
	rf.mu.Lock()
}

func (rf *raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *raft) handleInstallSnapshotReply(peer int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if rf.state != Leader || args.Term != rf.currentTerm.Get() {
		return
	}
	if reply.Term > rf.currentTerm.Get() {
		rf.currentTerm.Set(reply.Term)
		rf.votedFor.Set(-1)
		rf.state = Follower
		rf.resetElectionTimer()
		return
	}

	rf.nextIndex[peer] = args.LastIncludedIndex + 1
	rf.matchIndex[peer] = args.LastIncludedIndex
	rf.resetHeartbeatTimer()
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()

	if rf.state != Leader {
		rf.mu.Unlock()
		return -1, rf.currentTerm.Get(), false
	}

	entry := Entry{
		Term:    rf.currentTerm.Get(),
		Index:   rf.log.Len(),
		Command: command,
	}
	rf.log.Append(entry)

	rf.mu.Unlock()
	rf.broadcastHeartbeat(false)
	return entry.Index, entry.Term, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	close(rf.applyCh)
}

func (rf *raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *raft) ticker() {
	for !rf.killed() {
		select {
		case <-rf.electionTimer.C:
			rf.startElection()
		case <-rf.heartbeatTimer.C:
			rf.broadcastHeartbeat(true)
		}
	}
}

func (rf *raft) startElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state == Leader {
		return
	}
	rf.currentTerm.Set(rf.currentTerm.Get() + 1)
	rf.votedFor.Set(rf.me)
	rf.state = Candidate
	rf.resetElectionTimer()

	votesReceived := 1
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(server int, term int) {
			rf.mu.Lock()
			args := rf.makeRequestVoteArgs()
			var reply RequestVoteReply
			rf.mu.Unlock()
			if rf.sendRequestVote(server, args, &reply) {
				rf.mu.Lock()
				rf.handleRequestVoteReply(server, args, &reply, &votesReceived)
				rf.mu.Unlock()
			}
		}(peer, rf.currentTerm.Get())
	}
}

func (rf *raft) broadcastHeartbeat(isheartbeat bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return
	}

	rf.resetHeartbeatTimer()
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		if isheartbeat {
			go rf.replicateOnce(peer)
		} else {
			rf.replicateCond[peer].Signal()
		}
	}
}

func (rf *raft) applier() {
	defer func() { recover() }()

	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}

		commitIndex, lastApplied := rf.commitIndex, rf.lastApplied
		entries := rf.log.Range(lastApplied+1, commitIndex+1)
		rf.mu.Unlock()
		for _, entry := range entries {
			if rf.killed() {
				return
			}
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
			}
		}

		rf.mu.Lock()
		rf.lastApplied = max(rf.lastApplied, commitIndex)
		rf.mu.Unlock()
	}
}

func (rf *raft) replicator(peer int) {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.state != Leader || rf.nextIndex[peer] >= rf.log.Len() {
			rf.replicateCond[peer].Wait()
		}
		rf.mu.Unlock()
		rf.replicateOnce(peer)
	}
}

func (rf *raft) replicateOnce(peer int) {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	if rf.nextIndex[peer] <= rf.log.Base() {
		args := rf.makeInstallSnapshotArgs(peer)
		var reply InstallSnapshotReply
		rf.mu.Unlock()
		if rf.sendInstallSnapshot(peer, args, &reply) {
			rf.mu.Lock()
			rf.handleInstallSnapshotReply(peer, args, &reply)
			rf.mu.Unlock()
		}
	} else {
		args := rf.makeAppendEntriesArgs(peer)
		var reply AppendEntriesReply
		rf.mu.Unlock()
		if rf.sendAppendEntries(peer, args, &reply) {
			rf.mu.Lock()
			rf.handleAppendEntriesReply(peer, args, &reply)
			rf.mu.Unlock()
		}
	}
}

func (rf *raft) advanceCommitIndex() {
	for N := rf.log.Len() - 1; N > rf.commitIndex; N-- {
		if rf.log.Term(N) != rf.currentTerm.Get() {
			continue
		}
		count := 1
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= N {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = N
			rf.applyCond.Signal()
			break
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(servers []string, me int, snapshot *persist.Block, applyCh chan ApplyMsg) *raft {
	gob.Register(AppendEntriesArgs{})
	gob.Register(AppendEntriesReply{})
	gob.Register(RequestVoteArgs{})
	gob.Register(RequestVoteReply{})
	gob.Register(InstallSnapshotArgs{})
	gob.Register(InstallSnapshotReply{})

	rf := &raft{
		peers:     make([]*rpc.Client, len(servers)),
		me:        me,
		applyCh:   applyCh,

		state:          Follower,
		electionTimer:  time.NewTimer(electionTimeout()),
		heartbeatTimer: time.NewTimer(heartbeatTimeout()),
		replicateCond:  make([]*sync.Cond, len(servers)),

		currentTerm: persist.NewInt("currentTerm"),
		votedFor:    persist.NewInt("votedFor"),
		log:         NewLog(),
		snapshot:    snapshot,

		commitIndex: 0,
		lastApplied: 0,

		nextIndex:  make([]int, len(servers)),
		matchIndex: make([]int, len(servers)),
	}

	rf.commitIndex = rf.log.Base()
	rf.lastApplied = rf.log.Base()

	rf.applyCond = sync.NewCond(&rf.mu)
	go rf.applier()
	go rf.ticker()

	for i, server := range servers {
		rf.peers[i] = rpc.NewClient(server)
		rf.nextIndex[i], rf.matchIndex[i] = rf.log.Len(), 0
		if i != rf.me {
			rf.replicateCond[i] = sync.NewCond(&rf.mu)
			go rf.replicator(i)
		}
	}

	return rf
}

// timeout utils

func electionTimeout() time.Duration {
	ms := 700 + rand.Intn(300)
	return time.Duration(ms) * time.Millisecond
}

func (rf *raft) resetElectionTimer() {
	if !rf.electionTimer.Stop() {
		select {
		case <-rf.electionTimer.C:
		default:
		}
	}
	rf.electionTimer.Reset(electionTimeout())
}

func heartbeatTimeout() time.Duration {
	ms := 100
	return time.Duration(ms) * time.Millisecond
}

func (rf *raft) resetHeartbeatTimer() {
	if !rf.heartbeatTimer.Stop() {
		select {
		case <-rf.heartbeatTimer.C:
		default:
		}
	}
	rf.heartbeatTimer.Reset(heartbeatTimeout())
}
