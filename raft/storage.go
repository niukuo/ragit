package raft

import (
	"io"

	"go.etcd.io/etcd/raft"
	pb "go.etcd.io/etcd/raft/raftpb"
)

type InitialState struct {
	AppliedIndex uint64

	ConfState pb.ConfState
	ConfIndex uint64
}

type Storage interface {
	raft.Storage

	GetInitState() (*InitialState, error)
	Save(hardState pb.HardState, entries []pb.Entry) error

	Describe(w io.Writer)
}
