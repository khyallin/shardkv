package raft

import (
	"math/rand"
	"time"
)

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
