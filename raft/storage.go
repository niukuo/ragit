package raft

import (
	"io"

	"github.com/niukuo/ragit/refs"
	"go.etcd.io/etcd/raft"
	pb "go.etcd.io/etcd/raft/raftpb"
)

type InitialState struct {
	HardState    pb.HardState
	AppliedIndex uint64

	Peers     []refs.PeerID
	ConfIndex uint64
}

type Storage interface {
	raft.Storage

	GetInitState() (*InitialState, error)
	Save(hardState pb.HardState, entries []pb.Entry, sync bool) error

	Describe(w io.Writer)
}
