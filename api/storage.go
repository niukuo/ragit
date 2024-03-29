package api

import (
	"github.com/niukuo/ragit/refs"
	pb "go.etcd.io/etcd/raft/v3/raftpb"
)

type Storage interface {
	Snapshot() (pb.Snapshot, error)
	GetAllRefs() (map[string]refs.Hash, error)
}
