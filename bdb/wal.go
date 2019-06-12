package bdb

import pb "go.etcd.io/etcd/raft/raftpb"

type WALStorage interface {
	SaveWAL(ents []pb.Entry, sync bool) error
	Entries(lo, hi, maxSize uint64) ([]pb.Entry, error)
	Term(i uint64) (uint64, error)
	LastIndex() (uint64, error)
	FirstIndex() (uint64, error)
	Close()
}
