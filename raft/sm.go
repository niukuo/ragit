package raft

import (
	"context"

	"github.com/niukuo/ragit/refs"
	pb "go.etcd.io/etcd/raft/v3/raftpb"
)

type OpEvent struct {
	Term  uint64
	Index uint64
	Ops   []*refs.Oplog_Op
}

type StateMachine interface {
	OnStart() error

	OnLeaderStart(term uint64)
	OnLeaderStop()

	OnApply(term, index uint64, oplog *refs.Oplog, handle refs.ReqHandle) error
	OnConfState(index uint64, confState pb.ConfState, members []refs.Member, opType pb.ConfChangeType) error

	OnSnapshot(snapshot pb.Snapshot, srcId PeerID) error

	StartSubscriber(ctx context.Context, appliedIndex uint64, eventCh chan<- OpEvent) error

	WaitForApplyIndex(ctx context.Context, index uint64) error
}
