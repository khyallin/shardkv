package raft

import (
	"encoding/gob"

	"github.com/khyallin/shardkv/persist"
)

type Entry struct {
	Index   int
	Term    int
	Command interface{}
}

type Log struct {
	*persist.Array[Entry]
}

func NewLog() *Log {
	gob.Register(Entry{})
	a := persist.NewArray[Entry]("log")
	if a.Len() == 0 {
		a.Append(Entry{Index: 0, Term: 0, Command: nil})
	}
	return &Log{Array: a,}
}

func (l *Log) Len() int {
	return l.Base() + l.Array.Len()
}

func (l *Log) Entry(index int) Entry {
	return l.Item(index - l.Base())
}

func (l *Log) Base() int {
	return l.Item(0).Index
}

func (l *Log) Last() Entry {
	return l.Array.Item(l.Array.Len() - 1)
}

func (l *Log) Term(index int) int {
	return l.Entry(index).Term
}

func (l *Log) Command(index int) any {
	return l.Entry(index).Command
}

func (l *Log) Compact(index int) {
	l.Set(l.Tail(index))
}

func (l *Log) Truncate(index int) {
	l.Set(l.Head(index))
}

func (L *Log) Range(l, r int) []Entry {
	return L.Array.Range(l - L.Base(), r - L.Base())
}

func (l *Log) Head(index int) []Entry {
	return l.Range(l.Base(), index)
}

func (l *Log) Tail(index int) []Entry {
	return l.Range(index, l.Len())
}