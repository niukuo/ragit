package raft

import (
	"github.com/niukuo/ragit/refs"
	"go.etcd.io/etcd/raft"
	pb "go.etcd.io/etcd/raft/raftpb"
)

type Storage interface {
	raft.Storage

	Save(hardState pb.HardState, entries []pb.Entry, snapshot pb.Snapshot, sync bool) error

	Apply(term, index uint64, oplog refs.Oplog, outCh <-chan ApplyResult) error
	UpdateConfState(term, index uint64, confState pb.ConfState) error

	Close()
}
