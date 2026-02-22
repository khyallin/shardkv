package api

import "time"

type Tversion uint64

type PutArgs struct {
	Key     string
	Value   string
	Version Tversion
}

type PutReply struct {
	Err Err
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Value   string
	Version Tversion
	Err     Err
}

type StatusArgs struct {
}

type StatusReply struct {
	TotalQPS   float64
	DoneQPS    float64
	SuccessQPS float64
	MaxLatency time.Duration
	AvgLatency time.Duration
	Err        Err
}
