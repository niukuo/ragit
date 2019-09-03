package bdb

import (
	"io"

	pb "go.etcd.io/etcd/raft/raftpb"
)

type WALStorage interface {
	Describe(w io.Writer)
	SetSnapshotIndex(index uint64)
	SaveWAL(ents []pb.Entry, sync bool) error
	Close()

	Entries(lo, hi, maxSize uint64) ([]pb.Entry, error)
	Term(i uint64) (uint64, error)
	LastIndex() (uint64, error)
	FirstIndex() (uint64, error)
}
