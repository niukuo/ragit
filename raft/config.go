package raft

import (
	"time"

	"go.etcd.io/etcd/pkg/types"
	"go.etcd.io/etcd/raft"
)

type Config = *config
type config struct {
	TickDuration time.Duration
	ClusterID    types.ID
	raft.Config
}

func NewConfig() Config {
	return &config{
		TickDuration: 1 * time.Second,
		ClusterID:    0x1000,
	}
}
