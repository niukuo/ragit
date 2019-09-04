package raft

import (
	"context"

	"github.com/niukuo/ragit/refs"
	"go.etcd.io/etcd/raft"
	pb "go.etcd.io/etcd/raft/raftpb"
)

type Storage interface {
	raft.Storage

	Save(hardState pb.HardState, entries []pb.Entry, snapshot pb.Snapshot, srcId PeerID, sync bool) error

	Apply(ctx context.Context, term, index uint64, oplog refs.Oplog, srcId PeerID) error
	UpdateConfState(term, index uint64, confState pb.ConfState) error

	OnLeaderStart(term uint64)
	OnLeaderStop()

	Close()
}
