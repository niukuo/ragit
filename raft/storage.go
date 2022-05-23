package raft

import (
	"io"

	"github.com/niukuo/ragit/refs"
	"go.etcd.io/etcd/raft"
	pb "go.etcd.io/etcd/raft/raftpb"
)

type InitialState struct {
	AppliedIndex uint64

	ConfState pb.ConfState
	ConfIndex uint64
	LocalID   uint64
	Members   []*refs.Member
}

type Storage interface {
	raft.Storage

	GetInitState() (*InitialState, error)
	Save(hardState pb.HardState, entries []pb.Entry) error

	GetMemberAddrs(memberID refs.PeerID) ([]string, error)

	Describe(w io.Writer)

	OnConfIndexChange(confIndex uint64)
}
