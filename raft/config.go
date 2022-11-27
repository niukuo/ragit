package raft

import (
	"time"

	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/raft/v3"
)

type Config = *config
type config struct {
	TickDuration   time.Duration
	ClusterID      types.ID
	PeerListenURLs []string
	raft.Config
	StateMachine
	Storage
}

func NewConfig() Config {
	return &config{
		TickDuration: 1 * time.Second,
		ClusterID:    0x1000,
	}
}
