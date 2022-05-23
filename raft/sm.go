package raft

import (
	"context"

	"github.com/niukuo/ragit/refs"
	pb "go.etcd.io/etcd/raft/raftpb"
)

type StateMachine interface {
	OnStart() error

	OnLeaderStart(term uint64)
	OnLeaderStop()

	OnApply(ctx context.Context, term, index uint64, oplog refs.Oplog) error
	OnConfState(index uint64, confState pb.ConfState, member []*refs.Member, opType pb.ConfChangeType) error

	OnSnapshot(snapshot pb.Snapshot, srcId PeerID) error

	WaitForApplyIndex(ctx context.Context, index uint64) error
}
