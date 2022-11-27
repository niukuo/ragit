package raft

import (
	"io"

	"github.com/niukuo/ragit/refs"
	"go.etcd.io/etcd/raft/v3"
	pb "go.etcd.io/etcd/raft/v3/raftpb"
)

type InitialState struct {
	AppliedIndex uint64

	ConfState pb.ConfState
	ConfIndex uint64

	LocalID uint64
	Members []refs.Member
}

type Storage interface {
	raft.Storage

	GetInitState() (*InitialState, error)
	Save(hardState pb.HardState, entries []pb.Entry) error

	GetLeaderTerm() uint64
	GetAllRefs() (map[string]refs.Hash, error)
	GetAllMemberURLs() (map[PeerID][]string, error)
	GetURLsByMemberID(id PeerID) ([]string, error)

	GetConfState() (*pb.ConfState, error)

	Describe(w io.Writer)

	OnConfIndexChange(confIndex uint64)
}
